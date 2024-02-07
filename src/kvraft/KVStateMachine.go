package kvraft

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (mk *MemoryKV) Get(key string) (string, Err) {
	if val, ok := mk.KV[key]; ok {
		return val, OK
	}
	return "", ErrNoKey
}

func (mk *MemoryKV) Put(key, value string) Err {
	mk.KV[key] = value
	return OK
}

func (mk *MemoryKV) Append(key, value string) Err {
	mk.KV[key] += value
	return OK
}
