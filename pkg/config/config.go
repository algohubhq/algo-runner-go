package config

import (
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/openapi"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"regexp"
)

type ConfigLoader struct {
	Logger *logging.Logger
}

// NewConfigLoader returns a new ConfigLoader struct
func NewConfigLoader(logger *logging.Logger) ConfigLoader {
	return ConfigLoader{
		Logger: logger,
	}
}

func (cl *ConfigLoader) LoadConfigFromFile(fileName string) openapi.AlgoRunnerConfig {

	raw, err := ioutil.ReadFile(fileName)
	if err != nil {
		cl.Logger.LogMessage.Msg = fmt.Sprintf("Unable to read the config file [%s].", fileName)
		cl.Logger.Log(err)
	}

	var c openapi.AlgoRunnerConfig
	jsonErr := json.Unmarshal(raw, &c)

	if jsonErr != nil {
		cl.Logger.LogMessage.Msg = fmt.Sprintf("Unable to deserialize the config file [%s].", fileName)
		cl.Logger.Log(jsonErr)
	}

	return c

}

func (cl *ConfigLoader) LoadConfigFromString(jsonConfig string) openapi.AlgoRunnerConfig {

	var c openapi.AlgoRunnerConfig
	jsonErr := json.Unmarshal([]byte(jsonConfig), &c)

	if jsonErr != nil {
		cl.Logger.LogMessage.Msg = fmt.Sprintf("Unable to deserialize the config from string [%s].", jsonConfig)
		cl.Logger.Log(jsonErr)
	}

	return c

}

// parse url usually obtained from env.
func (cl *ConfigLoader) ParseEnvURL(envURL string) (*url.URL, string, string, error) {
	u, e := url.Parse(envURL)
	if e != nil {
		return nil, "", "", fmt.Errorf("S3 Endpoint url invalid [%s]", envURL)
	}

	var accessKey, secretKey string
	// Check if username:password is provided in URL, with no
	// access keys or secret we proceed and perform anonymous
	// requests.
	if u.User != nil {
		accessKey = u.User.Username()
		secretKey, _ = u.User.Password()
	}

	// Look for if URL has invalid values and return error.
	if !((u.Scheme == "http" || u.Scheme == "https") &&
		(u.Path == "/" || u.Path == "") && u.Opaque == "" &&
		!u.ForceQuery && u.RawQuery == "" && u.Fragment == "") {
		return nil, "", "", fmt.Errorf("S3 Endpoint url invalid [%s]", u.String())
	}

	// Now that we have validated the URL to be in expected style.
	u.User = nil

	return u, accessKey, secretKey, nil
}

// parse url usually obtained from env.
func (cl *ConfigLoader) ParseEnvURLStr(envURL string) (*url.URL, string, string, error) {
	var envURLStr string
	u, accessKey, secretKey, err := cl.ParseEnvURL(envURL)
	if err != nil {
		// url parsing can fail when accessKey/secretKey contains non url encoded values
		// such as #. Strip accessKey/secretKey from envURL and parse again.
		re := regexp.MustCompile("^(https?://)(.*?):(.*?)@(.*?)$")
		res := re.FindAllStringSubmatch(envURL, -1)
		// regex will return full match, scheme, accessKey, secretKey and endpoint:port as
		// captured groups.
		if res == nil || len(res[0]) != 5 {
			return nil, "", "", err
		}
		for k, v := range res[0] {
			if k == 2 {
				accessKey = v
			}
			if k == 3 {
				secretKey = v
			}
			if k == 1 || k == 4 {
				envURLStr = fmt.Sprintf("%s%s", envURLStr, v)
			}
		}
		u, _, _, err = cl.ParseEnvURL(envURLStr)
		if err != nil {
			return nil, "", "", err
		}
	}
	// Check if username:password is provided in URL, with no
	// access keys or secret we proceed and perform anonymous
	// requests.
	if u.User != nil {
		accessKey = u.User.Username()
		secretKey, _ = u.User.Password()
	}
	return u, accessKey, secretKey, nil
}
