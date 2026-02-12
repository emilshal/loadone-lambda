package metrics

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/sirupsen/logrus"
)

var (
	cwClient *cloudwatch.CloudWatch
	sess     *session.Session
)

// Initialize session and CloudWatch client in init function for reuse across Lambda invocations
func init() {
	sess = session.Must(session.NewSession())
	cwClient = cloudwatch.New(sess)
}

// PublishCounterMetric publishes a counter metric to CloudWatch
func PublishCounterMetric(metricName string, value float64) {
	_, err := cwClient.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String("SQSWorkerMetrics"),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String(metricName),
				Unit:       aws.String("Count"),
				Value:      aws.Float64(value),
				Timestamp:  aws.Time(time.Now()),
			},
		},
	})
	if err != nil {
		logrus.WithError(err).WithField("metric_name", metricName).Error("Failed to publish CloudWatch metric")
	}
}

// PublishHistogramMetric publishes a histogram (duration) metric to CloudWatch
func PublishHistogramMetric(metricName string, value float64) {
	_, err := cwClient.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace: aws.String("SQSWorkerMetrics"),
		MetricData: []*cloudwatch.MetricDatum{
			{
				MetricName: aws.String(metricName),
				Unit:       aws.String("Seconds"),
				Value:      aws.Float64(value),
				Timestamp:  aws.Time(time.Now()),
			},
		},
	})
	if err != nil {
		logrus.WithError(err).WithField("metric_name", metricName).Error("Failed to publish CloudWatch histogram metric")
	}
}

func IncrementMessagesReceived() {
	PublishCounterMetric("MessagesReceived", 1)
}

func IncrementMessagesProcessed() {
	PublishCounterMetric("MessagesProcessed", 1)
}

func IncrementMessagesFailed() {
	PublishCounterMetric("MessagesFailed", 1)
}

func IncrementMessagesParsed() {
	PublishCounterMetric("MessagesParsed", 1)
}

func ObserveProcessingDuration(durationSeconds float64) {
	PublishHistogramMetric("ProcessingDuration", durationSeconds)
}

func IncrementMessagesDeleted() {
	PublishCounterMetric("MessagesDeleted", 1)
}
