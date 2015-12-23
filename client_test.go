package statsd

import (
	"net"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func newLocalListenerUDP(t *testing.T) (*net.UDPConn, *net.UDPAddr) {
	udpAddr, err := net.ResolveUDPAddr("udp", ":1200")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	return ln, udpAddr
}

func TestTotal(t *testing.T) {
	ln, udpAddr := newLocalListenerUDP(t)
	defer ln.Close()

	prefix := "myproject."

	client := NewStatsdClient(udpAddr.String(), prefix, 100, time.Second)

	ch := make(chan string, 0)

	s := map[string]int64{
		"a:b:c": 5,
		"d:e:f": 2,
		"x:b:c": 5,
		"g.h.i": 1,
	}

	expected := make(map[string]int64)
	for k, v := range s {
		expected[k] = v
	}

	// also test %HOST% replacement
	s["zz.%HOST%"] = 1
	hostname, err := os.Hostname()
	expected["zz."+hostname] = 1

	go doListenUDP(ln, ch, len(s))

	err = client.CreateSocket()
	if nil != err {
		t.Fatal(err)
	}
	defer client.Close()

	for k, v := range s {
		client.Total(k, v)
	}

	actual := make(map[string]int64)

	re := regexp.MustCompile(`^(.*)\:(\d+)\|(\w).*$`)

	for i := len(s); i > 0; i-- {
		x := <-ch
		x = strings.TrimSpace(x)
		if !strings.HasPrefix(x, prefix) {
			t.Errorf("Metric without expected prefix: expected '%s', actual '%s'", prefix, x)
		}
		vv := re.FindStringSubmatch(x)
		if vv[3] != "t" {
			t.Errorf("Metric without expected suffix: expected 't', actual '%s'", vv[3])
		}
		v, err := strconv.ParseInt(vv[2], 10, 64)
		if err != nil {
			t.Error(err)
		}
		actual[vv[1][len(prefix):]] = v
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("did not receive all metrics: Expected: %T %v, Actual: %T %v ", expected, expected, actual, actual)
	}
}

func doListenUDP(conn *net.UDPConn, ch chan string, n int) {
	for n > 0 {
		buffer := make([]byte, 1400)
		size, err := conn.Read(buffer)
		if err != nil {
			panic(err)
		}
		for _, msg := range strings.Split(string(buffer[:size]), "\n") {
			ch <- msg
			n--
		}
	}
}
