package runtime

import (
	"bytes"
	"errors"
	"gopkg.in/yaml.v3"
	"io"
	"time"
)

// settings 保存从 probe 节读取的内部参数。
type settings struct {
	Port              int
	Interval, Timeout time.Duration
}

// parseSettings 严格解析单一对象，缺省端口 22222、间隔和超时各一秒，不读取环境变量。
func parseSettings(body []byte) (settings, error) {
	wire := struct {
		Port     int    `yaml:"port"`
		Interval string `yaml:"interval"`
		Timeout  string `yaml:"timeout"`
	}{22222, "1s", "1s"}
	if len(body) > 0 {
		var node yaml.Node
		if err := yaml.Unmarshal(body, &node); err != nil || len(node.Content) != 1 || node.Content[0].Kind != yaml.MappingNode || hasNull(&node) {
			return settings{}, errors.New("probe: 配置必须是非空值字段组成的 YAML 对象")
		}
		fields := node.Content[0].Content
		for i := 0; i < len(fields); i += 2 {
			key, value := fields[i], fields[i+1]
			if key.Tag != "!!str" || key.Value == "port" && value.Tag != "!!int" || (key.Value == "interval" || key.Value == "timeout") && value.Tag != "!!str" {
				return settings{}, errors.New("probe: 配置字段类型无效")
			}
		}
		decoder := yaml.NewDecoder(bytes.NewReader(body))
		decoder.KnownFields(true)
		if err := decoder.Decode(&wire); err != nil {
			return settings{}, errors.New("probe: 存在未知字段、重复字段或错误类型")
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF {
			return settings{}, errors.New("probe: 只允许一个 YAML 文档")
		}
	}
	interval, e1 := time.ParseDuration(wire.Interval)
	timeout, e2 := time.ParseDuration(wire.Timeout)
	if wire.Port < 1 || wire.Port > 65535 || e1 != nil || e2 != nil || interval <= 0 || timeout <= 0 {
		return settings{}, errors.New("probe: port 必须为 1..65535，interval 和 timeout 必须为正时长")
	}
	return settings{wire.Port, interval, timeout}, nil
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
