package data

import (
	"container/list"
	"sync"
)

type cmdElement struct {
	cmd      Command
	proposed bool
}

// CommandSet is a linkedhashset for Commands
type CommandSet struct {
	mut   sync.Mutex
	set   map[Command]*list.Element
	order list.List // 实际上只需要一个set就够了，但是为了实现指令执行的FIFO，这里用了list保证次序。
}

func NewCommandSet() *CommandSet {
	c := &CommandSet{
		set: make(map[Command]*list.Element),
	}
	c.order.Init()
	return c
}

// Add adds cmds to the set. Duplicate entries are ignored
func (s *CommandSet) Add(cmds ...Command) {
	s.mut.Lock()
	defer s.mut.Unlock()

	for _, cmd := range cmds {
		// avoid duplicates
		if _, ok := s.set[cmd]; ok {
			continue
		}
		e := s.order.PushBack(&cmdElement{cmd: cmd})
		s.set[cmd] = e
	}
}

// Remove removes cmds from the set
func (s *CommandSet) Remove(cmds ...Command) {
	s.mut.Lock()
	defer s.mut.Unlock()

	for _, cmd := range cmds {
		if e, ok := s.set[cmd]; ok {
			// remove e from list and set
			delete(s.set, cmd)
			s.order.Remove(e)
		}
	}
}

// 不移除地 返回n个Command，不够n个返回nil  // hotstuff专用
func (s *CommandSet) GetExactlyFirst(n int) []Command {
	if len(s.set) < n {
		return nil
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	if len(s.set) < n { // 2次判断，第1次为了防止多次加锁，第2次保证一定有n个指令。
		return nil
	}

	cmds := make([]Command, 0, n)
	i := 0
	e := s.order.Front()
	for i < n {
		if e == nil {
			break
		}
		if c := e.Value.(*cmdElement); !c.proposed {
			cmds = append(cmds, c.cmd)
			i++
		}
		e = e.Next()
	}
	return cmds
}

// 不移除地 返回 最多n个Command // hotstuff专用
func (s *CommandSet) GetFirst(n int) []Command {
	s.mut.Lock()
	defer s.mut.Unlock()

	if len(s.set) == 0 {
		return nil
	}

	cmds := make([]Command, 0, n)
	i := 0
	e := s.order.Front()
	for i < n {
		if e == nil {
			break
		}
		if c := e.Value.(*cmdElement); !c.proposed {
			cmds = append(cmds, c.cmd)
			i++
		}
		e = e.Next()
	}
	return cmds
}

// 移除 并 返回n个Command，不够n个返回nil
func (s *CommandSet) RetriveExactlyFirst(n int) []Command {
	if len(s.set) < n {
		return nil
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	if len(s.set) < n { // 2次判断，第1次为了防止多次加锁，第2次保证一定有n个指令。
		return nil
	}

	cmds := make([]Command, 0, n)
	i := 0
	cur := s.order.Front()
	next := cur
	for i < n {
		if cur == nil {
			break
		}
		if c := cur.Value.(*cmdElement); !c.proposed {
			cmds = append(cmds, c.cmd)
			i++
			delete(s.set, c.cmd)
			next = cur.Next()
			s.order.Remove(cur)
			cur = next
		} else {
			cur = cur.Next()
		}
	}
	return cmds
}

// 移除 并 返回最多n个Command
func (s *CommandSet) RetriveFirst(n int) []Command {
	s.mut.Lock()
	defer s.mut.Unlock()

	if len(s.set) == 0 {
		return nil
	}

	cmds := make([]Command, 0, n)
	i := 0
	cur := s.order.Front()
	next := cur
	for i < n {
		if cur == nil {
			break
		}
		if c := cur.Value.(*cmdElement); !c.proposed {
			cmds = append(cmds, c.cmd)
			i++
			delete(s.set, c.cmd)
			next = cur.Next()
			s.order.Remove(cur)
			cur = next
		} else {
			cur = cur.Next()
		}
	}
	return cmds
}

// Contains returns true if the set contains cmd, false otherwise
func (s *CommandSet) Contains(cmd Command) bool {
	s.mut.Lock()
	defer s.mut.Unlock()
	_, ok := s.set[cmd]
	return ok
}

// Len returns the length of the set
func (s *CommandSet) Len() int {
	s.mut.Lock()
	defer s.mut.Unlock()
	return len(s.set)
}

// TrimToLen will try to remove proposed elements from the set until its length is equal to or less than 'length'
func (s *CommandSet) TrimToLen(length int) {
	s.mut.Lock()
	defer s.mut.Unlock()

	e := s.order.Front()
	for length < len(s.set) {
		if e == nil {
			break
		}
		n := e.Next()
		c := e.Value.(*cmdElement)
		if c.proposed {
			s.order.Remove(e)
			delete(s.set, c.cmd)
		}
		e = n
	}
}

// IsProposed will return true if the given command is marked as proposed。proposed的不一定已经commit了，所以可能还会有点不安全。
func (s *CommandSet) IsProposed(cmd Command) bool {
	s.mut.Lock()
	defer s.mut.Unlock()
	if e, ok := s.set[cmd]; ok {
		return e.Value.(*cmdElement).proposed
	}
	return false
}

// MarkProposed will mark the given commands as proposed and move them to the back of the queue
func (s *CommandSet) MarkProposed(cmds ...Command) {
	s.mut.Lock()
	defer s.mut.Unlock()
	for _, cmd := range cmds {
		if e, ok := s.set[cmd]; ok {
			e.Value.(*cmdElement).proposed = true
			// Move to back so that it's not immediately deleted by a call to TrimToLen()
			s.order.MoveToBack(e)
		} else {
			// We don't have the command locally yet, so let's store it
			e := s.order.PushBack(&cmdElement{cmd: cmd, proposed: true})
			s.set[cmd] = e
		}
	}
}
