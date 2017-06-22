package canal

import (
	"sync"

	"github.com/ngaut/log"
)

type Pool struct {
	work chan dumpParseHandlerGo
	wg   sync.WaitGroup
}

func NewPool(maxGoroutines int) *Pool {
	log.Debug("Setup Pool  \n") //debug
	p := Pool{
		work: make(chan dumpParseHandlerGo),
	}

	p.wg.Add(maxGoroutines)
	for i := 0; i < maxGoroutines; i++ {
		log.Debug("Before Setup Pool thread ") // debug
		go func() {
			defer p.wg.Done()
			log.Debug("Setup Pool thread \n") //debug
			for w := range p.work {
				//fmt.Printf(" Into go Pool thread  \n") //debug
				w.DataGo()
			}
		}()
	}
	return &p
}

func (p *Pool) Run(w dumpParseHandlerGo) {
	p.work <- w
}

func (p *Pool) Shutdown() {
	close(p.work)
	p.wg.Wait()
}
