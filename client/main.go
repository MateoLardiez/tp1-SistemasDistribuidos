package main

import (
	"movies-analysis/client/common"
	"movies-analysis/client/config"
)

func main() {
	v, err := config.InitConfig()
	if err != nil {
		config.Log.Criticalf("%s", err)
	}

	if err := config.InitLogger(v.GetString("log.level")); err != nil {
		config.Log.Criticalf("%s", err)
	}

	// Print program config with debugging purposes
	config.PrintConfig(v)

	clientConfig := common.ClientConfig{
		ServerAddress:  v.GetString("server.address"),
		ID:             v.GetString("id"),
		LoopAmount:     v.GetInt("loop.amount"),
		LoopPeriod:     v.GetDuration("loop.period"),
		MaxAmount:      v.GetInt("batch.maxAmount"),
		Phase:          common.CODE_BATCH,
		FilesToProcess: v.GetStringSlice("files"), // Obtener la lista de archivos de la configuración
	}

	client := common.NewClient(clientConfig)
	client.StartClientLoop()
}
