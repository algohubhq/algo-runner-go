package storage

import (
	"algo-runner-go/pkg/logging"
	"algo-runner-go/pkg/openapi"
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/minio/minio-go/v6"
)

// Storage defines the S3 compatible storage configuration
type Storage struct {
	Uploader         *Uploader
	ConnectionString string
	Host             string
	AccessKeyID      string
	SecretAccessKey  string
	UseSSL           bool
}

type Uploader struct {
	HealthyChan chan<- bool
	Config      *openapi.AlgoRunnerConfig
	Logger      *logging.Logger
	client      *minio.Client
}

// New creates a new Storage struct
func NewStorage(healthyChan chan<- bool,
	config *openapi.AlgoRunnerConfig,
	connectionString string,
	logger *logging.Logger) *Storage {

	storage := Storage{}

	host, accessKey, secret, err := storage.ParseEnvURLStr(connectionString)
	if err != nil {
		logger.Error(fmt.Sprintf("S3 Connection String is not valid. [%s]", connectionString), errors.New("S3 Connection String malformed"))
	}

	storage.ConnectionString = connectionString
	storage.Host = host.Host
	storage.AccessKeyID = accessKey
	storage.SecretAccessKey = secret
	storage.UseSSL = host.Scheme == "https"

	uploader := storage.newUploader(config, &storage, logger, healthyChan)
	storage.Uploader = uploader

	return &storage

}

// New creates a new Uploader struct
func (s *Storage) newUploader(config *openapi.AlgoRunnerConfig,
	storageConfig *Storage,
	logger *logging.Logger,
	healthyChan chan<- bool) *Uploader {

	// Initialize minio client object.
	minioClient, err := minio.New(storageConfig.Host,
		storageConfig.AccessKeyID,
		storageConfig.SecretAccessKey,
		storageConfig.UseSSL)
	if err != nil {
		logger.Error("Error initializing minio client", err)
	}

	uploader := &Uploader{
		HealthyChan: healthyChan,
		client:      minioClient,
		Config:      config,
		Logger:      logger,
	}

	go s.createBucket(uploader)

	return uploader

}

func (s *Storage) createBucket(u *Uploader) {

	destBucket := strings.ToLower(fmt.Sprintf("algorun.%s.%s",
		u.Config.DeploymentOwner,
		u.Config.DeploymentName))
	// location := "us-east-1"

	err := u.client.MakeBucket(destBucket, "")
	if err != nil {
		// Check to see if we already own this bucket (which happens if you run this twice)
		exists, errBucketExists := u.client.BucketExists(destBucket)
		if errBucketExists != nil && !exists {
			u.Logger.Error(fmt.Sprintf("Error making bucket. [%s]", destBucket), err)
		}
	} else {
		u.Logger.Info(fmt.Sprintf("Successfully created bucket. [%s]", destBucket))
	}

}

func (u *Uploader) Upload(fileReference openapi.FileReference, byteData []byte) error {

	dataReader := bytes.NewReader(byteData)

	// Upload file with PutObject
	n, err := u.client.PutObject(fileReference.Bucket, fileReference.File, dataReader, int64(len(byteData)), minio.PutObjectOptions{})
	if err != nil {
		u.Logger.Error(fmt.Sprintf("Error uploading file [%s]", fileReference.File), err)
		u.HealthyChan <- false
		return err
	}

	u.Logger.Info(fmt.Sprintf("Successfully uploaded %s of size %d", fileReference.File, n))

	return err

}

// parse url usually obtained from env.
func (s *Storage) ParseEnvURL(envURL string) (*url.URL, string, string, error) {
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
func (s *Storage) ParseEnvURLStr(envURL string) (*url.URL, string, string, error) {
	var envURLStr string
	u, accessKey, secretKey, err := s.ParseEnvURL(envURL)
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
		u, _, _, err = s.ParseEnvURL(envURLStr)
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
