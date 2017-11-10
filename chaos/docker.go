package chaos

import (
	"context"
	"fmt"
	"os/exec"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

//cli, err := client.NewEnvClient()
//if err != nil {
//		panic(err)
//	}

//for _, container := range containers {
//	fmt.Println(container.NetworkSettings.Networks["dockerchaos_default"].IPAddress)
//	}

func assertRunning(cli *client.Client, expected []string) error {
	containers, err := cli.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return err
	}

	seen := make(map[string]struct{})
	for _, container := range containers {
		seen[container.Names[0]] = struct{}{}
	}
	var hit []string
	var miss []string
	for _, v := range expected {
		if _, ok := seen[v]; ok {
			hit = append(hit, v)
		} else {
			miss = append(miss, v)
		}
	}
	if len(miss) != 0 {
		return fmt.Errorf("missing containers %q. (found %q)", miss, hit)
	}
	return nil
}

// eg metrictank2
func launch(name string) error {
	cmd := exec.Command("docker-compose", "start", name)
	cmd.Dir = path("docker/docker-chaos")
	return cmd.Run()
}

// TODO: isolate only towards ip's of other instances, so we can still receive stats and query the api
func isolate(name, dur string) error {
	cmd := exec.Command("docker", "run", "--rm", "-v", "/var/run/docker.sock:/var/run/docker.sock", "pumba", "--", "pumba", "netem", "--tc-image", "gaiadocker/iproute2", "--duration", dur, "loss", "--percent", "100", name)
	return cmd.Start()
}
