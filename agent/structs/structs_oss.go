// +build !consulent

package structs

import (
	"hash"

	"github.com/hashicorp/consul/acl"
)

var emptyEnterpriseMeta = EnterpriseMeta{}

// EnterpriseMeta stub
type EnterpriseMeta struct{}

func (m *EnterpriseMeta) estimateSize() int {
	return 0
}

func (m *EnterpriseMeta) addToHash(_ hash.Hash, _ bool) {
	// do nothing
}

func (m *EnterpriseMeta) Matches(_ *EnterpriseMeta) bool {
	return true
}

func (m *EnterpriseMeta) IsSame(_ *EnterpriseMeta) bool {
	return true
}

func (m *EnterpriseMeta) LessThan(_ *EnterpriseMeta) bool {
	return false
}

// ReplicationEnterpriseMeta stub
func ReplicationEnterpriseMeta() *EnterpriseMeta {
	return &emptyEnterpriseMeta
}

// DefaultEnterpriseMeta stub
func DefaultEnterpriseMeta() *EnterpriseMeta {
	return &emptyEnterpriseMeta
}

// WildcardEnterpriseMeta stub
func WildcardEnterpriseMeta() *EnterpriseMeta {
	return &emptyEnterpriseMeta
}

// FillAuthzContext stub
func (_ *EnterpriseMeta) FillAuthzContext(_ *acl.EnterpriseAuthorizerContext) {}

func (_ *EnterpriseMeta) Normalize() {}

// FillAuthzContext stub
func (_ *DirEntry) FillAuthzContext(_ *acl.EnterpriseAuthorizerContext) {}

// FillAuthzContext stub
func (_ *RegisterRequest) FillAuthzContext(_ *acl.EnterpriseAuthorizerContext) {}

func (_ *RegisterRequest) GetEnterpriseMeta() *EnterpriseMeta {
	return nil
}

// OSS Stub
func (op *TxnNodeOp) FillAuthzContext(ctx *acl.EnterpriseAuthorizerContext) {}

// OSS Stub
func (_ *TxnServiceOp) FillAuthzContext(_ *acl.EnterpriseAuthorizerContext) {}

// OSS Stub
func (_ *TxnCheckOp) FillAuthzContext(_ *acl.EnterpriseAuthorizerContext) {}

func ServiceIDString(id string, _ *EnterpriseMeta) string {
	return id
}

func (sid *ServiceID) String() string {
	return sid.ID
}

func (cid *CheckID) String() string {
	return string(cid.ID)
}

func (_ *HealthCheck) Validate() error {
	return nil
}
