package runtime

import (
	"bytes"
	"errors"
	"io"
	"time"

	"gopkg.in/yaml.v3"
)

// settings 保存从 monitor 节读取的内部参数。
type settings struct {
	Port                          int
	Interval, Timeout             time.Duration
	SampleInterval, SampleTimeout time.Duration
}

// parseSettings 严格解析单一对象，缺省端口 9091、间隔和超时各一秒，不读取环境变量。
func parseSettings(body []byte) (settings, error) {
	wire := struct {
		Port           int    `yaml:"port"`
		Interval       string `yaml:"interval"`
		Timeout        string `yaml:"timeout"`
		SampleInterval string `yaml:"sample_interval"`
		SampleTimeout  string `yaml:"sample_timeout"`
	}{9091, "1s", "1s", "5s", "1s"}
	if len(body) > 0 {
		var node yaml.Node
		if err := yaml.Unmarshal(body, &node); err != nil || len(node.Content) != 1 || node.Content[0].Kind != yaml.MappingNode || hasNull(&node) {
			return settings{}, errors.New("monitor: 配置必须是非空值字段组成的 YAML 对象")
		}
		fields := node.Content[0].Content
		for i := 0; i < len(fields); i += 2 {
			key, value := fields[i], fields[i+1]
			if key.Tag != "!!str" || key.Value == "port" && value.Tag != "!!int" || (key.Value == "interval" || key.Value == "timeout" || key.Value == "sample_interval" || key.Value == "sample_timeout") && value.Tag != "!!str" {
				return settings{}, errors.New("monitor: 配置字段类型无效")
			}
		}
		decoder := yaml.NewDecoder(bytes.NewReader(body))
		decoder.KnownFields(true)
		if err := decoder.Decode(&wire); err != nil {
			return settings{}, errors.New("monitor: 存在未知字段、重复字段或错误类型")
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return settings{}, errors.New("monitor: 只允许一个 YAML 文档")
		}
	}
	interval, e1 := time.ParseDuration(wire.Interval)
	timeout, e2 := time.ParseDuration(wire.Timeout)
	sampleInterval, e3 := time.ParseDuration(wire.SampleInterval)
	sampleTimeout, e4 := time.ParseDuration(wire.SampleTimeout)
	if e3 != nil || e4 != nil || sampleInterval <= 0 || sampleTimeout <= 0 {
		return settings{}, errors.New("monitor: sample_interval 和 sample_timeout 必须为正时长")
	}
	if wire.Port < 1 || wire.Port > 65535 || e1 != nil || e2 != nil || interval <= 0 || timeout <= 0 {
		return settings{}, errors.New("monitor: port 必须为 1..65535，interval 和 timeout 必须为正时长")
	}
	return settings{wire.Port, interval, timeout, sampleInterval, sampleTimeout}, nil
}

// hasNull 拒绝被结构体解码静默当作默认值的 null。
func hasNull(node *yaml.Node) bool {
	if node.Tag == "!!null" {
		return true
	}
	for _, child := range node.Content {
		if hasNull(child) {
			return true
		}
	}
	return false
}
