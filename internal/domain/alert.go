package domain

type AlertStatus string

const (
	StatusPending     AlertStatus = "pending"
	StatusProcessing  AlertStatus = "processing"
	StatusDelivered   AlertStatus = "delivered"
	StatusFailed      AlertStatus = "failed"
	StatusCanceled    AlertStatus = "Canceled"
	StatusUnspecified AlertStatus = "Unspecified"
)

type AlertSeverity string

const (
	SeverityLow         AlertSeverity = "low"
	SeverityMedium      AlertSeverity = "medium"
	SeverityHigh        AlertSeverity = "high"
	SeverityUnspecified AlertSeverity = "unspecified"
)

type Alert struct {
	ID        string
	Message   string
	Severity  AlertSeverity
	Status    AlertStatus
	Attempts  int32
	CreatedAt int64
}
