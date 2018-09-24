package docker

import (
	"context"
	"os/exec"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/grafana/metrictank/stacktest/track"
	log "github.com/sirupsen/logrus"
)

var cli *client.Client

func init() {
	var err error
	cli, err = client.NewEnvClient()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Panic("failed to create new client")
	}
}

/* not currently used, but useful later

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
func start(name string) error {
	cmd := exec.Command("docker-compose", "start", name)
	cmd.Dir = path("docker/docker-chaos")
	return cmd.Run()
}

// eg metrictank2
func stop(name string) error {
	cmd := exec.Command("docker-compose", "stop", name)
	cmd.Dir = path("docker/docker-chaos")
	return cmd.Run()
}
*/

// Isolate isolates traffic between containers in setA and containers in setB
func Isolate(setA, setB []string, dur string) error {
	// note: isolateOut should return very fast (order of ms)
	// so we can run all this in serial
	for _, a := range setA {
		err := IsolateOut(a, dur, setB...)
		if err != nil {
			return err
		}
	}
	for _, b := range setB {
		err := IsolateOut(b, dur, setA...)
		if err != nil {
			return err
		}
	}
	return nil
}

// IsolateOut isolates traffic from the given docker container to all others matching the expression
func IsolateOut(name, dur string, targets ...string) error {
	containers, err := cli.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return err
	}
	targetSet := make(map[string]struct{})
	for _, target := range targets {
		targetSet["dockerchaos_"+target+"_1"] = struct{}{}
	}
	var ips []string
	name = "dockerchaos_" + name + "_1"

	for _, container := range containers {
		containerName := container.Names[0][1:] // docker puts a "/" in front of each name. not sure why
		if _, ok := targetSet[containerName]; ok {
			ips = append(ips, container.NetworkSettings.Networks["dockerchaos_default"].IPAddress)
		}
	}
	var cmd *exec.Cmd
	if len(ips) > 0 {
		t := strings.Join(ips, ",")
		cmd = exec.Command("docker", "run", "--rm", "-v", "/var/run/docker.sock:/var/run/docker.sock", "gaiaadm/pumba", "--", "pumba", "netem", "--target", t, "--tc-image", "gaiadocker/iproute2", "--duration", dur, "loss", "--percent", "100", name)
	} else {
		cmd = exec.Command("docker", "run", "--rm", "-v", "/var/run/docker.sock:/var/run/docker.sock", "gaiaadm/pumba", "--", "pumba", "netem", "--tc-image", "gaiadocker/iproute2", "--duration", dur, "loss", "--percent", "100", name)
	}

	// log all pumba's output
	_, err = track.NewTracker(cmd, false, false, "pumba-stdout", "pumba-stderr")
	if err != nil {
		return err
	}

	return cmd.Start()
}
