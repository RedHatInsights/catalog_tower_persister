package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"

	clowder "github.com/redhatinsights/app-common-go/pkg/api/v1"

	"github.com/spf13/viper"
)

// TowerPersisterConfig represents the runtime configuration
type TowerPersisterConfig struct {
	Hostname             string
	DatabaseHostname     string
	DatabasePort         int
	DatabaseName         string
	DatabaseUsername     string
	DatabasePassword     string
	KafkaBrokers         []string
	KafkaGroupID         string
	KafkaTopic           string
	WebPort              int
	MetricsPort          int
	Profile              bool
	OpenshiftBuildCommit string
	Version              string
	LogGroup             string
	LogLevel             string
	AwsRegion            string
	AwsAccessKeyId       string
	AwsSecretAccessKey   string
	Debug                bool
	DebugUserAgent       *regexp.Regexp
	UseClowder           bool
}

// Get returns an initialized IngressConfig
func Get() *TowerPersisterConfig {

	options := viper.New()

	if os.Getenv("CLOWDER_ENABLED") == "true" {
		cfg := clowder.LoadedConfig

		options.SetDefault("DatabaseHostname", cfg.Database.Hostname)
		options.SetDefault("DatabasePort", cfg.Database.Port)
		options.SetDefault("DatabaseName", cfg.Database.Name)
		options.SetDefault("DatabaseUsername", cfg.Database.Username)
		options.SetDefault("DatabasePassword", cfg.Database.Password)
		options.SetDefault("WebPort", cfg.WebPort)
		options.SetDefault("MetricsPort", cfg.MetricsPort)
		options.SetDefault("KafkaBrokers", fmt.Sprintf("%s:%v", cfg.Kafka.Brokers[0].Hostname, *cfg.Kafka.Brokers[0].Port))
		options.SetDefault("LogGroup", cfg.Logging.Cloudwatch.LogGroup)
		options.SetDefault("AwsRegion", cfg.Logging.Cloudwatch.Region)
		options.SetDefault("AwsAccessKeyId", cfg.Logging.Cloudwatch.AccessKeyId)
		options.SetDefault("AwsSecretAccessKey", cfg.Logging.Cloudwatch.SecretAccessKey)
	} else {
		options.SetDefault("WebPort", 3000)
		options.SetDefault("MetricsPort", 8080)
		kafkaBroker := fmt.Sprintf("%s:%s", os.Getenv("QUEUE_HOST"), os.Getenv("QUEUE_PORT"))
		options.SetDefault("KafkaBrokers", []string{kafkaBroker})
		options.SetDefault("LogGroup", "platform-dev")
		options.SetDefault("AwsRegion", "us-east-1")
		options.SetDefault("AwsAccessKeyId", os.Getenv("CW_AWS_ACCESS_KEY_ID"))
		options.SetDefault("AwsSecretAccessKey", os.Getenv("CW_AWS_SECRET_ACCESS_KEY"))
		options.SetDefault("DatabaseHostname", os.Getenv("DATABASE_HOST"))
		port, err := strconv.Atoi(os.Getenv("DATABASE_PORT"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error converting Database Port %v\n", err)
		} else {
			options.SetDefault("DatabasePort", port)
		}
		options.SetDefault("DatabaseUsername", os.Getenv("DATABASE_USER"))
		options.SetDefault("DatabasePassword", os.Getenv("DATABASE_PASSWORD"))
		options.SetDefault("DatabaseName", os.Getenv("DATABASE_NAME"))

	}

	options.SetDefault("KafkaTopic", "platform.catalog.persister")
	options.SetDefault("KafkaGroupID", "tower_persister")
	options.SetDefault("LogLevel", "INFO")
	options.SetDefault("OpenshiftBuildCommit", "notrunninginopenshift")
	options.SetDefault("Profile", false)
	options.SetDefault("Debug", false)
	options.SetDefault("DebugUserAgent", `unspecified`)
	options.SetEnvPrefix("TOWER_PERSISTER")
	options.AutomaticEnv()
	kubenv := viper.New()
	kubenv.SetDefault("Openshift_Build_Commit", "notrunninginopenshift")
	kubenv.SetDefault("Hostname", "Hostname_Unavailable")
	kubenv.AutomaticEnv()

	return &TowerPersisterConfig{
		Hostname:             kubenv.GetString("Hostname"),
		DatabaseHostname:     options.GetString("DatabaseHostname"),
		DatabasePort:         options.GetInt("DatabasePort"),
		DatabaseName:         options.GetString("DatabaseName"),
		DatabaseUsername:     options.GetString("DatabaseUsername"),
		DatabasePassword:     options.GetString("DatabasePassword"),
		KafkaBrokers:         options.GetStringSlice("KafkaBrokers"),
		KafkaGroupID:         options.GetString("KafkaGroupID"),
		KafkaTopic:           options.GetString("KafkaTopic"),
		WebPort:              options.GetInt("WebPort"),
		MetricsPort:          options.GetInt("MetricsPort"),
		Profile:              options.GetBool("Profile"),
		Debug:                options.GetBool("Debug"),
		DebugUserAgent:       regexp.MustCompile(options.GetString("DebugUserAgent")),
		OpenshiftBuildCommit: kubenv.GetString("Openshift_Build_Commit"),
		Version:              "1.0.0",
		LogGroup:             options.GetString("LogGroup"),
		LogLevel:             options.GetString("LogLevel"),
		AwsRegion:            options.GetString("AwsRegion"),
		AwsAccessKeyId:       options.GetString("AwsAccessKeyId"),
		AwsSecretAccessKey:   options.GetString("AwsSecretAccessKey"),
		UseClowder:           os.Getenv("CLOWDER_ENABLED") == "true",
	}
}
