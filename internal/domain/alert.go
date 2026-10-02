package domain

type AlertStatus string

const (
	StatusPending     AlertStatus = "pending"
	StatusProcessing  AlertStatus = "processing"
	StatusDelivered   AlertStatus = "delivered"
	StatusFailed      AlertStatus = "failed"
	StatusCanceled    AlertStatus = "canceled"
	StatusUnspecified AlertStatus = "unspecified"
)

type AlertSeverity string

const (
	SeverityLow         AlertSeverity = "low"
	SeverityMedium      AlertSeverity = "medium"
	SeverityHigh        AlertSeverity = "high"
	SeverityCritical    AlertSeverity = "critical"
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

func IsTerminal(status AlertStatus) bool {
	switch status {
	case StatusDelivered, StatusFailed, StatusCanceled:
		return true
	default:
		return false
	}
}

func (s AlertSeverity) IsValid() bool {
	switch s {
	case SeverityLow, SeverityMedium, SeverityHigh, SeverityCritical:
		return true
	default:
		return false
	}
}
