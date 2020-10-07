package repo

import (

    "github.com/plan-systems/plan-go/ctx"

)


type vaultMgr struct {
    ctx.Context

    host *host
}


func newVaultMgr(host *host) *vaultMgr {
    return &vaultMgr{
        host: host,
    }
}

// Start -- see interface Domain
func (vm *vaultMgr) Start() error {
    err := vm.CtxStart(
        vm.ctxStartup,
        nil,
        nil,
        vm.ctxStopping,
    )
    return err
}


func (vm *vaultMgr) ctxStartup() error {

    vm.SetLogLabel("vault mgr")

    // TODO: startup vault access here
    return nil

}


func (vm *vaultMgr) ctxStopping() {

    // TODO: initiate vault access shutdown
}

