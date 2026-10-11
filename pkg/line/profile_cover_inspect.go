package line

// Cover inspection is read-only and never returns a LINE profile value,
// token, MID, picture, cover object ID, or raw response body.
import (
	"context"
	"encoding/json"
)

type ProfileCoverInspection struct {
	HasCoverInfo bool `json:"has_cover_info"`
	HasHomeID bool `json:"has_home_id"`
	HasObjectID bool `json:"has_object_id"`
	HasOID bool `json:"has_oid"`
	HasNamespace bool `json:"has_namespace"`
	HasSID bool `json:"has_sid"`
	IsDefaultCover bool `json:"is_default_cover"`
	ParsedSelfCover bool `json:"parsed_self_cover"`
}

// Only GetProfile, issueChannelToken and GET timeline/getCover are used.
// This is independent from, and never bypasses, the write protocol guard.
func (c *Client) InspectProfileCoverContext(ctx context.Context, selfMID string) (ProfileCoverInspection, error) {
	info := ProfileCoverInspection{}
	region, err := c.profileCoverSelfRegion(ctx, selfMID)
	if err != nil { return info, err }
	token, err := c.acquireProfileChannelTokenContext(ctx)
	if err != nil { return info, err }
	raw, err := c.profileCoverRequest(ctx, selfMID, region, token, "")
	if err != nil { return info, err }
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return info, profileWriteError(ProfileWriteUnknown, "PROFILE_COVER_RESPONSE_INVALID", nil)
	}
	_, info.HasHomeID = object["homeId"]
	_ = json.Unmarshal(object["isDefaultCover"], &info.IsDefaultCover)
	var attrs map[string]json.RawMessage
	if json.Unmarshal(object["coverObsInfo"], &attrs) == nil && attrs != nil {
		info.HasCoverInfo = true
		_, info.HasObjectID = attrs["objectId"]
		_, info.HasOID = attrs["oid"]
		_, info.HasNamespace = attrs["obsNamespace"]
		_, info.HasSID = attrs["sid"]
	}
	if _, err = parseProfileCover(raw, selfMID); err != nil { return info, err }
	info.ParsedSelfCover = true
	return info, nil
}
