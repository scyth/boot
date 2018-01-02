package boot

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

type RestartPolicy int

const (
	POLICY_QUIT         RestartPolicy = 0
	POLICY_RESTART      RestartPolicy = 1
	POLICY_RESTART_NEXT RestartPolicy = 2
)

type Service interface {
	BootStart(id BootId, appCfg interface{}, bootErrCh chan<- BootErr)
	BootStop(wg *sync.WaitGroup) // call wg.Done() after exit
	BootClearState()             // in case of restart, this method will be called to cleanup the state
}

type BootId int

func (id BootId) String() string {
	return strconv.FormatUint(uint64(id), 10)
}

type BootErr struct {
	Id  BootId
	Err error
}

func (be BootErr) String() string {
	return fmt.Sprintf("bootErr, svc(%d): %s", be.Id, be.Err.Error())
}

func (be BootErr) Error() string {
	return be.String()
}

func NewBootManager(appConfig interface{}) *BootManager {
	return &BootManager{
		appConfig:    appConfig,
		services:     make(serviceList, 0),
		servicesById: make(serviceMap),
		errCh:        make(chan BootErr, 0),
	}
}

type BootManager struct {
	svcCounter   uint64
	appConfig    interface{}
	services     serviceList
	servicesById serviceMap
	errCh        chan BootErr
	abortCh      <-chan bool
	doneCh       chan bool
}

func (bm *BootManager) Add(svc Service, name string, startPriority int, stopPriority int, restartPolicy RestartPolicy) {

	id := atomic.AddUint64(&bm.svcCounter, 1)
	item := &serviceItem{
		id:            BootId(id),
		name:          name,
		service:       svc,
		startPriority: startPriority,
		stopPriority:  stopPriority,
		restartPolicy: restartPolicy,
	}
	bm.services = append(bm.services, item)
	bm.servicesById[BootId(id)] = item
}

func (bm *BootManager) Run(abortCh <-chan bool) <-chan bool {
	bm.doneCh = make(chan bool, 0)
	go func() {
		for _, svc := range bm.services.byStartPriority() {
			svc.service.BootStart(svc.id, bm.appConfig, bm.errCh)
			svc.running = true
		}

		var e BootErr
	L:
		for {
			select {
			case e = <-bm.errCh:
				// service exited
				svc, ok := bm.servicesById[e.Id]
				if ok {
					log.Printf("boot: service '%s' exited with error, %s", svc.name, e)
					svc.running = false

					switch svc.restartPolicy {
					case POLICY_QUIT:
						bm.stopServices(nil)
						break L
					case POLICY_RESTART:
						svc.service.BootClearState()
						svc.service.BootStart(svc.id, bm.appConfig, bm.errCh)
						svc.running = true
					case POLICY_RESTART_NEXT:
						log.Printf("boot() stopping chained services due to restart policy")
						nextChain := bm.findNextChain(svc)
						bm.stopServices(nextChain)

						log.Printf("boot() restarting service '%s'", svc.name)
						svc.service.BootClearState()
						svc.service.BootStart(svc.id, bm.appConfig, bm.errCh)
						svc.running = true

						log.Printf("boot() restarting chained services")
						for _, s := range nextChain.byStartPriority() {
							s.service.BootClearState()
							s.service.BootStart(s.id, bm.appConfig, bm.errCh)
							s.running = true
						}
					default:
						log.Printf("boot: service restart policy unknown, aborting")
						bm.stopServices(nil)
						break L
					}
				} else {
					log.Printf("boot: service exited with error, but could not be identified with provided id")
					bm.stopServices(nil)
					break L
				}
			case <-bm.abortCh:
				// user requested abort, lets stop all services
				bm.stopServices(nil)
				break L
			}
		}
		close(bm.doneCh)
	}()
	return bm.doneCh
}

func (bm *BootManager) findNextChain(svc *serviceItem) serviceList {
	result := make(serviceList, 0)
	for _, s := range bm.services.byStartPriority() {
		if s.startPriority > svc.startPriority {
			result = append(result, s)
		}
	}
	return result
}
func (bm *BootManager) stopServices(chain serviceList) {
	if chain == nil {
		chain = bm.services
	}
	wg := new(sync.WaitGroup)
	for _, svc := range chain.byStopPriority() {
		if !svc.running {
			continue
		}
		log.Printf("boot() stopping service %s", svc.name)
		wg.Add(1)
		svc.service.BootStop(wg)
		wg.Wait()
		svc.running = false
	}
}

func (bm *BootManager) stopUpperChain(svc *serviceItem) serviceList {
	// bm.services.byStartPriority()

	return nil
}

type serviceItem struct {
	id                BootId
	name              string
	service           Service
	startPriority     int
	stopPriority      int
	running           bool
	constructorParams interface{}
	restartPolicy     RestartPolicy
}

type serviceList []*serviceItem

func (sl serviceList) byStartPriority() serviceList {
	result := make(serviceList, len(sl))
	copy(result, sl)
	sort.Slice(result, func(i, j int) bool { return result[i].startPriority < result[j].startPriority })
	return result
}
func (sl serviceList) byStopPriority() serviceList {
	result := make(serviceList, len(sl))
	copy(result, sl)
	sort.Slice(result, func(i, j int) bool { return result[i].stopPriority < result[j].stopPriority })
	return result
}

type serviceMap map[BootId]*serviceItem
