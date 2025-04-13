package ju

// 这是 OrderMap 的一个泛型版本

// Element is an element of a null terminated (none circular) intrusive doubly linked list that contains the key of the correspondent element in the ordered map too.
type Element[K comparable, V any] struct {
	Key   K
	Value V
	prev  *Element[K, V]
	next  *Element[K, V]
}

// Next returns the next list element or nil.
func (e *Element[K, V]) Next() *Element[K, V] {
	return e.next
}

// Prev returns the previous list element or nil.
func (e *Element[K, V]) Prev() *Element[K, V] {
	return e.prev
}

// list represents a null terminated (none circular) intrusive doubly linked list.
// The list is immediately usable after instantiation without the need of a dedicated initialization.
type list[K comparable, V any] struct {
	root Element[K, V] // list head and tail
}

func (l *list[K, V]) IsEmpty() bool {
	return l.root.next == nil
}

// Front returns the first element of list l or nil if the list is empty.
func (l *list[K, V]) Front() *Element[K, V] {
	return l.root.next
}

// Back returns the last element of list l or nil if the list is empty.
func (l *list[K, V]) Back() *Element[K, V] {
	return l.root.prev
}

// Remove removes e from its list
func (l *list[K, V]) Remove(e *Element[K, V]) {
	if e.prev == nil {
		l.root.next = e.next
	} else {
		e.prev.next = e.next
	}
	if e.next == nil {
		l.root.prev = e.prev
	} else {
		e.next.prev = e.prev
	}
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
}

// PushFront inserts a new element e with value v at the front of list l and returns e.
func (l *list[K, V]) PushFront(key K, value V) *Element[K, V] {
	e := &Element[K, V]{Key: key, Value: value}
	if l.root.next == nil {
		// It's the first element
		l.root.next = e
		l.root.prev = e
		return e
	}

	e.next = l.root.next
	l.root.next.prev = e
	l.root.next = e
	return e
}

// PushBack inserts a new element e with value v at the back of list l and returns e.
func (l *list[K, V]) PushBack(key K, value V) *Element[K, V] {
	e := &Element[K, V]{Key: key, Value: value}
	if l.root.prev == nil {
		// It's the first element
		l.root.next = e
		l.root.prev = e
		return e
	}

	e.prev = l.root.prev
	l.root.prev.next = e
	l.root.prev = e
	return e
}

type OrderMap[K comparable, V any] struct {
	kv map[K]*Element[K, V]
	ll list[K, V]
}

func NewOrderMap[K comparable, V any]() *OrderMap[K, V] {
	return &OrderMap[K, V]{
		kv: make(map[K]*Element[K, V]),
	}
}

// NewOrderMapWithCapacity creates a map with enough pre-allocated space to
// hold the specified number of elements.
func NewOrderMapWithCapacity[K comparable, V any](capacity int) *OrderMap[K, V] {
	return &OrderMap[K, V]{
		kv: make(map[K]*Element[K, V], capacity),
	}
}

// Get returns the value for a key. If the key does not exist, the second return
// parameter will be false and the value will be nil.
func (m *OrderMap[K, V]) Get(key K) (V, bool) {
	element, ok := m.kv[key]
	if ok {
		return element.Value, true
	}
	var v V
	return v, false
}

// Set will set (or replace) a value for a key. If the key was new, then true
// will be returned. The returned value will be false if the value was replaced
// (even if the value was the same).
func (m *OrderMap[K, V]) Set(key K, value V) bool {
	_, alreadyExist := m.kv[key]
	if alreadyExist {
		m.kv[key].Value = value
		return false
	}

	element := m.ll.PushBack(key, value)
	m.kv[key] = element
	return true
}

// GetOrDefault returns the value for a key. If the key does not exist, returns
// the default value instead.
func (m *OrderMap[K, V]) GetOrDefault(key K, defaultValue V) V {
	if element, ok := m.kv[key]; ok {
		return element.Value
	}

	return defaultValue
}

// GetElement returns the element for a key. If the key does not exist, the
// pointer will be nil.
func (m *OrderMap[K, V]) GetElement(key K) *Element[K, V] {
	element, ok := m.kv[key]
	if ok {
		return element
	}

	return nil
}

// Len returns the number of elements in the map.
func (m *OrderMap[K, V]) Len() int {
	return len(m.kv)
}

// Keys returns all the keys in the order they were inserted. If a key was
// replaced it will retain the same position. To ensure most recently set keys
// are always at the end you must always Delete before Set.
func (m *OrderMap[K, V]) Keys() (keys []K) {
	keys = make([]K, 0, m.Len())
	for el := m.Front(); el != nil; el = el.Next() {
		keys = append(keys, el.Key)
	}
	return keys
}
func (m *OrderMap[K, V]) Values() (values []V) {
	values = make([]V, 0, m.Len())
	for el := m.Front(); el != nil; el = el.Next() {
		values = append(values, el.Value)
	}
	return values
}

// Delete will remove a key from the map. It will return true if the key was
// removed (the key did exist).
func (m *OrderMap[K, V]) Delete(key K) (didDelete bool) {
	element, ok := m.kv[key]
	if ok {
		m.ll.Remove(element)
		delete(m.kv, key)
	}

	return ok
}

// Front will return the element that is the first (oldest Set element). If
// there are no elements this will return nil.
func (m *OrderMap[K, V]) Front() *Element[K, V] {
	return m.ll.Front()
}

// Back will return the element that is the last (most recent Set element). If
// there are no elements this will return nil.
func (m *OrderMap[K, V]) Back() *Element[K, V] {
	return m.ll.Back()
}

// Copy returns a new OrderMap with the same elements.
// Using Copy while there are concurrent writes may mangle the result.
func (m *OrderMap[K, V]) Copy() *OrderMap[K, V] {
	m2 := NewOrderMapWithCapacity[K, V](m.Len())

	for el := m.Front(); el != nil; el = el.Next() {
		m2.Set(el.Key, el.Value)
	}

	return m2
}

// Has checks if a key exists in the map.
func (m *OrderMap[K, V]) Has(key K) bool {
	_, exists := m.kv[key]
	return exists
}
