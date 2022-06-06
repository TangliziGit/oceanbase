// Code generated by entc, DO NOT EDIT.

package ent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/oceanbase/configserver/ent/obcluster"
	"github.com/oceanbase/configserver/ent/predicate"

	"entgo.io/ent"
)

const (
	// Operation types.
	OpCreate    = ent.OpCreate
	OpDelete    = ent.OpDelete
	OpDeleteOne = ent.OpDeleteOne
	OpUpdate    = ent.OpUpdate
	OpUpdateOne = ent.OpUpdateOne

	// Node types.
	TypeObCluster = "ObCluster"
)

// ObClusterMutation represents an operation that mutates the ObCluster nodes in the graph.
type ObClusterMutation struct {
	config
	op               Op
	typ              string
	id               *int
	create_time      *time.Time
	update_time      *time.Time
	name             *string
	ob_cluster_id    *int64
	addob_cluster_id *int64
	_type            *string
	rootservice_json *string
	clearedFields    map[string]struct{}
	done             bool
	oldValue         func(context.Context) (*ObCluster, error)
	predicates       []predicate.ObCluster
}

var _ ent.Mutation = (*ObClusterMutation)(nil)

// obclusterOption allows management of the mutation configuration using functional options.
type obclusterOption func(*ObClusterMutation)

// newObClusterMutation creates new mutation for the ObCluster entity.
func newObClusterMutation(c config, op Op, opts ...obclusterOption) *ObClusterMutation {
	m := &ObClusterMutation{
		config:        c,
		op:            op,
		typ:           TypeObCluster,
		clearedFields: make(map[string]struct{}),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// withObClusterID sets the ID field of the mutation.
func withObClusterID(id int) obclusterOption {
	return func(m *ObClusterMutation) {
		var (
			err   error
			once  sync.Once
			value *ObCluster
		)
		m.oldValue = func(ctx context.Context) (*ObCluster, error) {
			once.Do(func() {
				if m.done {
					err = errors.New("querying old values post mutation is not allowed")
				} else {
					value, err = m.Client().ObCluster.Get(ctx, id)
				}
			})
			return value, err
		}
		m.id = &id
	}
}

// withObCluster sets the old ObCluster of the mutation.
func withObCluster(node *ObCluster) obclusterOption {
	return func(m *ObClusterMutation) {
		m.oldValue = func(context.Context) (*ObCluster, error) {
			return node, nil
		}
		m.id = &node.ID
	}
}

// Client returns a new `ent.Client` from the mutation. If the mutation was
// executed in a transaction (ent.Tx), a transactional client is returned.
func (m ObClusterMutation) Client() *Client {
	client := &Client{config: m.config}
	client.init()
	return client
}

// Tx returns an `ent.Tx` for mutations that were executed in transactions;
// it returns an error otherwise.
func (m ObClusterMutation) Tx() (*Tx, error) {
	if _, ok := m.driver.(*txDriver); !ok {
		return nil, errors.New("ent: mutation is not running in a transaction")
	}
	tx := &Tx{config: m.config}
	tx.init()
	return tx, nil
}

// ID returns the ID value in the mutation. Note that the ID is only available
// if it was provided to the builder or after it was returned from the database.
func (m *ObClusterMutation) ID() (id int, exists bool) {
	if m.id == nil {
		return
	}
	return *m.id, true
}

// IDs queries the database and returns the entity ids that match the mutation's predicate.
// That means, if the mutation is applied within a transaction with an isolation level such
// as sql.LevelSerializable, the returned ids match the ids of the rows that will be updated
// or updated by the mutation.
func (m *ObClusterMutation) IDs(ctx context.Context) ([]int, error) {
	switch {
	case m.op.Is(OpUpdateOne | OpDeleteOne):
		id, exists := m.ID()
		if exists {
			return []int{id}, nil
		}
		fallthrough
	case m.op.Is(OpUpdate | OpDelete):
		return m.Client().ObCluster.Query().Where(m.predicates...).IDs(ctx)
	default:
		return nil, fmt.Errorf("IDs is not allowed on %s operations", m.op)
	}
}

// SetCreateTime sets the "create_time" field.
func (m *ObClusterMutation) SetCreateTime(t time.Time) {
	m.create_time = &t
}

// CreateTime returns the value of the "create_time" field in the mutation.
func (m *ObClusterMutation) CreateTime() (r time.Time, exists bool) {
	v := m.create_time
	if v == nil {
		return
	}
	return *v, true
}

// OldCreateTime returns the old "create_time" field's value of the ObCluster entity.
// If the ObCluster object wasn't provided to the builder, the object is fetched from the database.
// An error is returned if the mutation operation is not UpdateOne, or the database query fails.
func (m *ObClusterMutation) OldCreateTime(ctx context.Context) (v time.Time, err error) {
	if !m.op.Is(OpUpdateOne) {
		return v, errors.New("OldCreateTime is only allowed on UpdateOne operations")
	}
	if m.id == nil || m.oldValue == nil {
		return v, errors.New("OldCreateTime requires an ID field in the mutation")
	}
	oldValue, err := m.oldValue(ctx)
	if err != nil {
		return v, fmt.Errorf("querying old value for OldCreateTime: %w", err)
	}
	return oldValue.CreateTime, nil
}

// ResetCreateTime resets all changes to the "create_time" field.
func (m *ObClusterMutation) ResetCreateTime() {
	m.create_time = nil
}

// SetUpdateTime sets the "update_time" field.
func (m *ObClusterMutation) SetUpdateTime(t time.Time) {
	m.update_time = &t
}

// UpdateTime returns the value of the "update_time" field in the mutation.
func (m *ObClusterMutation) UpdateTime() (r time.Time, exists bool) {
	v := m.update_time
	if v == nil {
		return
	}
	return *v, true
}

// OldUpdateTime returns the old "update_time" field's value of the ObCluster entity.
// If the ObCluster object wasn't provided to the builder, the object is fetched from the database.
// An error is returned if the mutation operation is not UpdateOne, or the database query fails.
func (m *ObClusterMutation) OldUpdateTime(ctx context.Context) (v time.Time, err error) {
	if !m.op.Is(OpUpdateOne) {
		return v, errors.New("OldUpdateTime is only allowed on UpdateOne operations")
	}
	if m.id == nil || m.oldValue == nil {
		return v, errors.New("OldUpdateTime requires an ID field in the mutation")
	}
	oldValue, err := m.oldValue(ctx)
	if err != nil {
		return v, fmt.Errorf("querying old value for OldUpdateTime: %w", err)
	}
	return oldValue.UpdateTime, nil
}

// ResetUpdateTime resets all changes to the "update_time" field.
func (m *ObClusterMutation) ResetUpdateTime() {
	m.update_time = nil
}

// SetName sets the "name" field.
func (m *ObClusterMutation) SetName(s string) {
	m.name = &s
}

// Name returns the value of the "name" field in the mutation.
func (m *ObClusterMutation) Name() (r string, exists bool) {
	v := m.name
	if v == nil {
		return
	}
	return *v, true
}

// OldName returns the old "name" field's value of the ObCluster entity.
// If the ObCluster object wasn't provided to the builder, the object is fetched from the database.
// An error is returned if the mutation operation is not UpdateOne, or the database query fails.
func (m *ObClusterMutation) OldName(ctx context.Context) (v string, err error) {
	if !m.op.Is(OpUpdateOne) {
		return v, errors.New("OldName is only allowed on UpdateOne operations")
	}
	if m.id == nil || m.oldValue == nil {
		return v, errors.New("OldName requires an ID field in the mutation")
	}
	oldValue, err := m.oldValue(ctx)
	if err != nil {
		return v, fmt.Errorf("querying old value for OldName: %w", err)
	}
	return oldValue.Name, nil
}

// ResetName resets all changes to the "name" field.
func (m *ObClusterMutation) ResetName() {
	m.name = nil
}

// SetObClusterID sets the "ob_cluster_id" field.
func (m *ObClusterMutation) SetObClusterID(i int64) {
	m.ob_cluster_id = &i
	m.addob_cluster_id = nil
}

// ObClusterID returns the value of the "ob_cluster_id" field in the mutation.
func (m *ObClusterMutation) ObClusterID() (r int64, exists bool) {
	v := m.ob_cluster_id
	if v == nil {
		return
	}
	return *v, true
}

// OldObClusterID returns the old "ob_cluster_id" field's value of the ObCluster entity.
// If the ObCluster object wasn't provided to the builder, the object is fetched from the database.
// An error is returned if the mutation operation is not UpdateOne, or the database query fails.
func (m *ObClusterMutation) OldObClusterID(ctx context.Context) (v int64, err error) {
	if !m.op.Is(OpUpdateOne) {
		return v, errors.New("OldObClusterID is only allowed on UpdateOne operations")
	}
	if m.id == nil || m.oldValue == nil {
		return v, errors.New("OldObClusterID requires an ID field in the mutation")
	}
	oldValue, err := m.oldValue(ctx)
	if err != nil {
		return v, fmt.Errorf("querying old value for OldObClusterID: %w", err)
	}
	return oldValue.ObClusterID, nil
}

// AddObClusterID adds i to the "ob_cluster_id" field.
func (m *ObClusterMutation) AddObClusterID(i int64) {
	if m.addob_cluster_id != nil {
		*m.addob_cluster_id += i
	} else {
		m.addob_cluster_id = &i
	}
}

// AddedObClusterID returns the value that was added to the "ob_cluster_id" field in this mutation.
func (m *ObClusterMutation) AddedObClusterID() (r int64, exists bool) {
	v := m.addob_cluster_id
	if v == nil {
		return
	}
	return *v, true
}

// ResetObClusterID resets all changes to the "ob_cluster_id" field.
func (m *ObClusterMutation) ResetObClusterID() {
	m.ob_cluster_id = nil
	m.addob_cluster_id = nil
}

// SetType sets the "type" field.
func (m *ObClusterMutation) SetType(s string) {
	m._type = &s
}

// GetType returns the value of the "type" field in the mutation.
func (m *ObClusterMutation) GetType() (r string, exists bool) {
	v := m._type
	if v == nil {
		return
	}
	return *v, true
}

// OldType returns the old "type" field's value of the ObCluster entity.
// If the ObCluster object wasn't provided to the builder, the object is fetched from the database.
// An error is returned if the mutation operation is not UpdateOne, or the database query fails.
func (m *ObClusterMutation) OldType(ctx context.Context) (v string, err error) {
	if !m.op.Is(OpUpdateOne) {
		return v, errors.New("OldType is only allowed on UpdateOne operations")
	}
	if m.id == nil || m.oldValue == nil {
		return v, errors.New("OldType requires an ID field in the mutation")
	}
	oldValue, err := m.oldValue(ctx)
	if err != nil {
		return v, fmt.Errorf("querying old value for OldType: %w", err)
	}
	return oldValue.Type, nil
}

// ResetType resets all changes to the "type" field.
func (m *ObClusterMutation) ResetType() {
	m._type = nil
}

// SetRootserviceJSON sets the "rootservice_json" field.
func (m *ObClusterMutation) SetRootserviceJSON(s string) {
	m.rootservice_json = &s
}

// RootserviceJSON returns the value of the "rootservice_json" field in the mutation.
func (m *ObClusterMutation) RootserviceJSON() (r string, exists bool) {
	v := m.rootservice_json
	if v == nil {
		return
	}
	return *v, true
}

// OldRootserviceJSON returns the old "rootservice_json" field's value of the ObCluster entity.
// If the ObCluster object wasn't provided to the builder, the object is fetched from the database.
// An error is returned if the mutation operation is not UpdateOne, or the database query fails.
func (m *ObClusterMutation) OldRootserviceJSON(ctx context.Context) (v string, err error) {
	if !m.op.Is(OpUpdateOne) {
		return v, errors.New("OldRootserviceJSON is only allowed on UpdateOne operations")
	}
	if m.id == nil || m.oldValue == nil {
		return v, errors.New("OldRootserviceJSON requires an ID field in the mutation")
	}
	oldValue, err := m.oldValue(ctx)
	if err != nil {
		return v, fmt.Errorf("querying old value for OldRootserviceJSON: %w", err)
	}
	return oldValue.RootserviceJSON, nil
}

// ResetRootserviceJSON resets all changes to the "rootservice_json" field.
func (m *ObClusterMutation) ResetRootserviceJSON() {
	m.rootservice_json = nil
}

// Where appends a list predicates to the ObClusterMutation builder.
func (m *ObClusterMutation) Where(ps ...predicate.ObCluster) {
	m.predicates = append(m.predicates, ps...)
}

// Op returns the operation name.
func (m *ObClusterMutation) Op() Op {
	return m.op
}

// Type returns the node type of this mutation (ObCluster).
func (m *ObClusterMutation) Type() string {
	return m.typ
}

// Fields returns all fields that were changed during this mutation. Note that in
// order to get all numeric fields that were incremented/decremented, call
// AddedFields().
func (m *ObClusterMutation) Fields() []string {
	fields := make([]string, 0, 6)
	if m.create_time != nil {
		fields = append(fields, obcluster.FieldCreateTime)
	}
	if m.update_time != nil {
		fields = append(fields, obcluster.FieldUpdateTime)
	}
	if m.name != nil {
		fields = append(fields, obcluster.FieldName)
	}
	if m.ob_cluster_id != nil {
		fields = append(fields, obcluster.FieldObClusterID)
	}
	if m._type != nil {
		fields = append(fields, obcluster.FieldType)
	}
	if m.rootservice_json != nil {
		fields = append(fields, obcluster.FieldRootserviceJSON)
	}
	return fields
}

// Field returns the value of a field with the given name. The second boolean
// return value indicates that this field was not set, or was not defined in the
// schema.
func (m *ObClusterMutation) Field(name string) (ent.Value, bool) {
	switch name {
	case obcluster.FieldCreateTime:
		return m.CreateTime()
	case obcluster.FieldUpdateTime:
		return m.UpdateTime()
	case obcluster.FieldName:
		return m.Name()
	case obcluster.FieldObClusterID:
		return m.ObClusterID()
	case obcluster.FieldType:
		return m.GetType()
	case obcluster.FieldRootserviceJSON:
		return m.RootserviceJSON()
	}
	return nil, false
}

// OldField returns the old value of the field from the database. An error is
// returned if the mutation operation is not UpdateOne, or the query to the
// database failed.
func (m *ObClusterMutation) OldField(ctx context.Context, name string) (ent.Value, error) {
	switch name {
	case obcluster.FieldCreateTime:
		return m.OldCreateTime(ctx)
	case obcluster.FieldUpdateTime:
		return m.OldUpdateTime(ctx)
	case obcluster.FieldName:
		return m.OldName(ctx)
	case obcluster.FieldObClusterID:
		return m.OldObClusterID(ctx)
	case obcluster.FieldType:
		return m.OldType(ctx)
	case obcluster.FieldRootserviceJSON:
		return m.OldRootserviceJSON(ctx)
	}
	return nil, fmt.Errorf("unknown ObCluster field %s", name)
}

// SetField sets the value of a field with the given name. It returns an error if
// the field is not defined in the schema, or if the type mismatched the field
// type.
func (m *ObClusterMutation) SetField(name string, value ent.Value) error {
	switch name {
	case obcluster.FieldCreateTime:
		v, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("unexpected type %T for field %s", value, name)
		}
		m.SetCreateTime(v)
		return nil
	case obcluster.FieldUpdateTime:
		v, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("unexpected type %T for field %s", value, name)
		}
		m.SetUpdateTime(v)
		return nil
	case obcluster.FieldName:
		v, ok := value.(string)
		if !ok {
			return fmt.Errorf("unexpected type %T for field %s", value, name)
		}
		m.SetName(v)
		return nil
	case obcluster.FieldObClusterID:
		v, ok := value.(int64)
		if !ok {
			return fmt.Errorf("unexpected type %T for field %s", value, name)
		}
		m.SetObClusterID(v)
		return nil
	case obcluster.FieldType:
		v, ok := value.(string)
		if !ok {
			return fmt.Errorf("unexpected type %T for field %s", value, name)
		}
		m.SetType(v)
		return nil
	case obcluster.FieldRootserviceJSON:
		v, ok := value.(string)
		if !ok {
			return fmt.Errorf("unexpected type %T for field %s", value, name)
		}
		m.SetRootserviceJSON(v)
		return nil
	}
	return fmt.Errorf("unknown ObCluster field %s", name)
}

// AddedFields returns all numeric fields that were incremented/decremented during
// this mutation.
func (m *ObClusterMutation) AddedFields() []string {
	var fields []string
	if m.addob_cluster_id != nil {
		fields = append(fields, obcluster.FieldObClusterID)
	}
	return fields
}

// AddedField returns the numeric value that was incremented/decremented on a field
// with the given name. The second boolean return value indicates that this field
// was not set, or was not defined in the schema.
func (m *ObClusterMutation) AddedField(name string) (ent.Value, bool) {
	switch name {
	case obcluster.FieldObClusterID:
		return m.AddedObClusterID()
	}
	return nil, false
}

// AddField adds the value to the field with the given name. It returns an error if
// the field is not defined in the schema, or if the type mismatched the field
// type.
func (m *ObClusterMutation) AddField(name string, value ent.Value) error {
	switch name {
	case obcluster.FieldObClusterID:
		v, ok := value.(int64)
		if !ok {
			return fmt.Errorf("unexpected type %T for field %s", value, name)
		}
		m.AddObClusterID(v)
		return nil
	}
	return fmt.Errorf("unknown ObCluster numeric field %s", name)
}

// ClearedFields returns all nullable fields that were cleared during this
// mutation.
func (m *ObClusterMutation) ClearedFields() []string {
	return nil
}

// FieldCleared returns a boolean indicating if a field with the given name was
// cleared in this mutation.
func (m *ObClusterMutation) FieldCleared(name string) bool {
	_, ok := m.clearedFields[name]
	return ok
}

// ClearField clears the value of the field with the given name. It returns an
// error if the field is not defined in the schema.
func (m *ObClusterMutation) ClearField(name string) error {
	return fmt.Errorf("unknown ObCluster nullable field %s", name)
}

// ResetField resets all changes in the mutation for the field with the given name.
// It returns an error if the field is not defined in the schema.
func (m *ObClusterMutation) ResetField(name string) error {
	switch name {
	case obcluster.FieldCreateTime:
		m.ResetCreateTime()
		return nil
	case obcluster.FieldUpdateTime:
		m.ResetUpdateTime()
		return nil
	case obcluster.FieldName:
		m.ResetName()
		return nil
	case obcluster.FieldObClusterID:
		m.ResetObClusterID()
		return nil
	case obcluster.FieldType:
		m.ResetType()
		return nil
	case obcluster.FieldRootserviceJSON:
		m.ResetRootserviceJSON()
		return nil
	}
	return fmt.Errorf("unknown ObCluster field %s", name)
}

// AddedEdges returns all edge names that were set/added in this mutation.
func (m *ObClusterMutation) AddedEdges() []string {
	edges := make([]string, 0, 0)
	return edges
}

// AddedIDs returns all IDs (to other nodes) that were added for the given edge
// name in this mutation.
func (m *ObClusterMutation) AddedIDs(name string) []ent.Value {
	return nil
}

// RemovedEdges returns all edge names that were removed in this mutation.
func (m *ObClusterMutation) RemovedEdges() []string {
	edges := make([]string, 0, 0)
	return edges
}

// RemovedIDs returns all IDs (to other nodes) that were removed for the edge with
// the given name in this mutation.
func (m *ObClusterMutation) RemovedIDs(name string) []ent.Value {
	return nil
}

// ClearedEdges returns all edge names that were cleared in this mutation.
func (m *ObClusterMutation) ClearedEdges() []string {
	edges := make([]string, 0, 0)
	return edges
}

// EdgeCleared returns a boolean which indicates if the edge with the given name
// was cleared in this mutation.
func (m *ObClusterMutation) EdgeCleared(name string) bool {
	return false
}

// ClearEdge clears the value of the edge with the given name. It returns an error
// if that edge is not defined in the schema.
func (m *ObClusterMutation) ClearEdge(name string) error {
	return fmt.Errorf("unknown ObCluster unique edge %s", name)
}

// ResetEdge resets all changes to the edge with the given name in this mutation.
// It returns an error if the edge is not defined in the schema.
func (m *ObClusterMutation) ResetEdge(name string) error {
	return fmt.Errorf("unknown ObCluster edge %s", name)
}