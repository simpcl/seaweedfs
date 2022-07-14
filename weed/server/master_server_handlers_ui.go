package weed_server

import (
	"net/http"

	ui "weed/server/master_ui"
	"weed/stats"
	"weed/util"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = util.VERSION
	args := struct {
		Version  string
		Topology interface{}
		Leader   string
		Peers    interface{}
		Stats    map[string]interface{}
		Counters *stats.ServerStats
	}{
		util.VERSION,
		ms.Topo.ToMap(),
		ms.Topo.RaftServer.Leader(),
		ms.Topo.RaftServer.Peers(),
		infos,
		serverStats,
	}
	ui.StatusTpl.Execute(w, args)
}
