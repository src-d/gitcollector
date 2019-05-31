package gitcollector

// Job represents a message containing the repository required data to be download.
type Job struct {
	Endpoints []string
	IsFork    bool
}

// NewJob builds a new Job.
func NewJob(endpoints []string, isFork bool) *Job {
	return &Job{
		Endpoints: endpoints,
		IsFork:    isFork,
	}
}
