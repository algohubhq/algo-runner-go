package main

import (
	"algo-runner-go/openapi"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"regexp"
)

func loadConfigFromFile(fileName string) openapi.AlgoRunnerConfig {

	// Create the base log message
	localLog := logMessage{
		Type:    "Local",
		Version: "1",
	}

	raw, err := ioutil.ReadFile(fileName)
	if err != nil {
		localLog.Msg = fmt.Sprintf("Unable to read the config file [%s].", fileName)
		localLog.log(err)
	}

	var c openapi.AlgoRunnerConfig
	jsonErr := json.Unmarshal(raw, &c)

	if jsonErr != nil {
		localLog.Msg = fmt.Sprintf("Unable to deserialize the config file [%s].", fileName)
		localLog.log(jsonErr)
	}

	return c

}

func loadConfigFromString(jsonConfig string) openapi.AlgoRunnerConfig {

	// Create the base log message
	localLog := logMessage{
		Type:    "Local",
		Version: "1",
	}

	var c openapi.AlgoRunnerConfig
	jsonErr := json.Unmarshal([]byte(jsonConfig), &c)

	if jsonErr != nil {
		localLog.Msg = fmt.Sprintf("Unable to deserialize the config from string [%s].", jsonConfig)
		localLog.log(jsonErr)
	}

	return c

}

// parse url usually obtained from env.
func parseEnvURL(envURL string) (*url.URL, string, string, error) {
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
func parseEnvURLStr(envURL string) (*url.URL, string, string, error) {
	var envURLStr string
	u, accessKey, secretKey, err := parseEnvURL(envURL)
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
		u, _, _, err = parseEnvURL(envURLStr)
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
