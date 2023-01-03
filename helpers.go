package realis

import (
	"context"

	"github.com/aurora-scheduler/gorealis/v2/gen-go/apache/aurora"
)

func (r *Client) jobExists(key aurora.JobKey) (bool, error) {
	resp, err := r.client.GetConfigSummary(context.TODO(), &key)
	if err != nil {
		return false, err
	}

	return !(resp == nil ||
			resp.GetResult_() == nil ||
			resp.GetResult_().GetConfigSummaryResult_() == nil ||
			resp.GetResult_().GetConfigSummaryResult_().GetSummary() == nil ||
			resp.GetResponseCode() != aurora.ResponseCode_OK),
		nil
}
