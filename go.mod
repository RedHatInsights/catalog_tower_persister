module github.com/RedHatInsights/catalog_tower_persister

go 1.14

require (
	github.com/DATA-DOG/go-sqlmock v1.5.0
	github.com/aws/aws-sdk-go v1.36.25
	github.com/confluentinc/confluent-kafka-go v1.5.2 // indirect
	github.com/google/uuid v1.1.2
	github.com/redhatinsights/app-common-go v0.0.0-20201209144413-30bb68eb9891
	github.com/redhatinsights/platform-go-middlewares v0.7.0
	github.com/sirupsen/logrus v1.7.0
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.5.1
	gopkg.in/confluentinc/confluent-kafka-go.v1 v1.5.2
	gorm.io/datatypes v1.0.0
	gorm.io/driver/postgres v1.0.5
	gorm.io/gorm v1.20.7
)
