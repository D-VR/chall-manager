package lock

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ctfer-io/chall-manager/global"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	// lockOpTimeout bounds etcd lock operations so they can still finish even if the
	// originating RPC was canceled.
	lockOpTimeout = 15 * time.Second
)

// The etcd distributed lock enables you to have a powerful mutual exclusion (mutex)
// system in with an etcd cluster.
// You can't use a file lock in distributed environments such as Kubernetes because
// the Pods are isolated in there own contexts hence would not share the filelock
// information.
//
// This implementation goes further than a simple mutex, as it implements the
// readers-writer lock for a writer-preference.
//
// Based upon 'Concurrent Control with "Readers" and "Writers"' by Courtois et al. (1971)
// DOI: 10.1145/362759.362813
type EtcdRWLock struct {
	key string
	s   *concurrency.Session

	// readCounter  -> /chall-manager/<key>/readCounter
	// writeCounter -> /chall-manager/<key>/writeCounter
	m1, m2, m3, r, w *concurrency.Mutex
	// m1 -> /chall-manager/<key>/m1
	// m2 -> /chall-manager/<key>/m2
	// m3 -> /chall-manager/<key>/m3
	// r  -> /chall-manager/<key>/r
	// w  -> /chall-manager/<key>/w
}

func NewEtcdRWLock(ctx context.Context, key string) (RWLock, error) {
	s, err := global.GetEtcdManager().NewConcurrencySession(ctx)
	if err != nil {
		return nil, err
	}

	pfx := "/chall-manager/" + key + "/"
	return &EtcdRWLock{
		key: key,
		s:   s,
		m1:  concurrency.NewMutex(s, pfx+"m1"),
		m2:  concurrency.NewMutex(s, pfx+"m2"),
		m3:  concurrency.NewMutex(s, pfx+"m3"),
		r:   concurrency.NewMutex(s, pfx+"r"),
		w:   concurrency.NewMutex(s, pfx+"w"),
	}, nil
}

func (lock *EtcdRWLock) Key() string {
	return lock.key
}

func (lock *EtcdRWLock) RLock(ctx context.Context) error {
	etcdCli := global.GetEtcdManager()
	ctx, cancel := lockCtx(ctx)
	defer cancel()

	if err := lock.m3.Lock(ctx); err != nil {
		return err
	}
	defer unlock(ctx, lock.m3)

	if err := lock.r.Lock(ctx); err != nil {
		return err
	}
	defer unlock(ctx, lock.r)

	if err := lock.m1.Lock(ctx); err != nil {
		return err
	}
	defer unlock(ctx, lock.m1)

	k := fmt.Sprintf("/chall-manager/%s/readCounter", lock.key)
	res, err := etcdCli.Get(ctx, k)
	if err != nil {
		return err
	}
	var readCounter int
	switch len(res.Kvs) {
	case 0:
		readCounter = 0
	case 1:
		str := string(res.Kvs[0].Value)
		readCounter, err = strconv.Atoi(str)
		if err != nil {
			return errors.New("invalid format for " + k + ", got " + str)
		}
	default:
		return errors.New("invalid etcd filter for " + k)
	}
	readCounter++
	_, perr := etcdCli.Put(ctx, k, strconv.Itoa(readCounter))
	// Don't return perr for now, let's avoid race conditions and starvations

	if readCounter == 1 {
		if err := lock.w.Lock(ctx); err != nil {
			return err
		}
	}

	return perr
}

func (lock *EtcdRWLock) RUnlock(ctx context.Context) error {
	etcdCli := global.GetEtcdManager()
	ctx, cancel := lockCtx(ctx)
	defer cancel()

	if err := lock.m1.Lock(ctx); err != nil {
		return err
	}
	defer unlock(ctx, lock.m1)

	k := fmt.Sprintf("/chall-manager/%s/readCounter", lock.key)
	res, err := etcdCli.Get(ctx, k)
	if err != nil {
		return err
	}
	var readCounter int
	switch len(res.Kvs) {
	case 1:
		str := string(res.Kvs[0].Value)
		readCounter, err = strconv.Atoi(str)
		if err != nil {
			return errors.New("invalid format for " + k + ", got " + str)
		}
	default:
		return errors.New("invalid etcd filter for " + k)
	}
	readCounter--
	_, perr := etcdCli.Put(ctx, k, strconv.Itoa(readCounter))
	// Don't return perr for now, let's avoid race conditions and starvations

	if readCounter == 0 {
		if err := lock.w.Unlock(ctx); err != nil {
			return err
		}
	}

	return perr
}

func (lock *EtcdRWLock) RWLock(ctx context.Context) error {
	etcdCli := global.GetEtcdManager()
	ctx, cancel := lockCtx(ctx)
	defer cancel()

	if err := lock.m2.Lock(ctx); err != nil {
		return err
	}
	defer unlock(ctx, lock.m2)

	k := fmt.Sprintf("/chall-manager/%s/writeCounter", lock.key)
	res, err := etcdCli.Get(ctx, k)
	if err != nil {
		return err
	}
	var writeCounter int
	switch len(res.Kvs) {
	case 0:
		writeCounter = 0
	case 1:
		str := string(res.Kvs[0].Value)
		writeCounter, err = strconv.Atoi(str)
		if err != nil {
			return errors.New("invalid format for " + k + ", got " + str)
		}
	default:
		return errors.New("invalid etcd filter for " + k)
	}
	nextCounter := writeCounter + 1

	acquiredR := false
	if writeCounter == 0 {
		if err := lock.r.Lock(ctx); err != nil {
			return err
		}
		acquiredR = true
	}

	if err := lock.w.Lock(ctx); err != nil {
		if acquiredR {
			_ = lock.r.Unlock(ctx)
		}
		return err
	}

	_, perr := etcdCli.Put(ctx, k, strconv.Itoa(nextCounter))
	if perr != nil {
		// Roll back to avoid leaving the lock held without a counter update
		_ = lock.w.Unlock(ctx)
		if acquiredR {
			_ = lock.r.Unlock(ctx)
		}
		return perr
	}

	return nil
}

func (lock *EtcdRWLock) RWUnlock(ctx context.Context) error {
	etcdCli := global.GetEtcdManager()
	ctx, cancel := lockCtx(ctx)
	defer cancel()

	if err := lock.w.Unlock(ctx); err != nil {
		return err
	}

	if err := lock.m2.Lock(ctx); err != nil {
		return err
	}
	defer unlock(ctx, lock.m2)

	k := fmt.Sprintf("/chall-manager/%s/writeCounter", lock.key)
	res, err := etcdCli.Get(ctx, k)
	if err != nil {
		return err
	}
	var writeCounter int
	switch len(res.Kvs) {
	case 1:
		str := string(res.Kvs[0].Value)
		writeCounter, err = strconv.Atoi(str)
		if err != nil {
			return errors.New("invalid format for " + k + ", got " + str)
		}
	default:
		return errors.New("invalid etcd filter for " + k)
	}
	writeCounter--
	_, perr := etcdCli.Put(ctx, k, strconv.Itoa(writeCounter))
	// Don't return perr for now, let's avoid race conditions and starvations

	if writeCounter == 0 {
		if err := lock.r.Unlock(ctx); err != nil {
			return err
		}
	}

	return perr
}

func (lock *EtcdRWLock) Close() error {
	return lock.s.Close()
}

// lockCtx returns a non-cancelable, bounded context so lock/unlock can still
// reach etcd even if the parent RPC context was canceled upstream.
func lockCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx = context.WithoutCancel(ctx)
	ctx, cancel := context.WithTimeout(ctx, lockOpTimeout)
	return ctx, cancel
}

func unlock(ctx context.Context, mx *concurrency.Mutex) {
	if err := mx.Unlock(ctx); err != nil {
		global.Log().Error(ctx, "failed to unlock etcd mutex",
			zap.Error(err),
			zap.String("key", mx.Key()),
		)
	}
}
