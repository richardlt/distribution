package proxy

import (
	"context"

	"github.com/docker/distribution"
)

// proxyTagService supports local and remote lookup of tags.
type proxyTagService struct {
	localTags      distribution.TagService
	remoteTags     distribution.TagService
	authChallenger authChallenger
}

var _ distribution.TagService = proxyTagService{}

// always get first from local, if err try to get from remote then tag locally
func (pt proxyTagService) Get(ctx context.Context, tag string) (distribution.Descriptor, error) {
	desc, err := pt.localTags.Get(ctx, tag)
	if err == nil {
		return desc, nil
	}

	if err := pt.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return distribution.Descriptor{}, err
	}
	desc, err = pt.remoteTags.Get(ctx, tag)
	if err != nil {
		return distribution.Descriptor{}, err
	}
	if err := pt.localTags.Tag(ctx, tag, desc); err != nil {
		return distribution.Descriptor{}, err
	}

	return desc, nil
}

func (pt proxyTagService) Tag(ctx context.Context, tag string, desc distribution.Descriptor) error {
	return pt.localTags.Tag(ctx, tag, desc)
}

func (pt proxyTagService) Untag(ctx context.Context, tag string) error {
	err := pt.localTags.Untag(ctx, tag)
	if err != nil {
		return err
	}
	return nil
}

func (pt proxyTagService) All(ctx context.Context) ([]string, error) {
	return pt.localTags.All(ctx)
}

func (pt proxyTagService) Lookup(ctx context.Context, digest distribution.Descriptor) ([]string, error) {
	return pt.localTags.Lookup(ctx, digest)
}
