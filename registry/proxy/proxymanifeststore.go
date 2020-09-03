package proxy

import (
	"context"
	"time"

	"github.com/docker/distribution"
	"github.com/docker/distribution/reference"
	"github.com/opencontainers/go-digest"
)

// todo(richardscothern): from cache control header or config
const repositoryTTL = 24 * 7 * time.Hour

type proxyManifestStore struct {
	ctx             context.Context
	localManifests  distribution.ManifestService
	remoteManifests distribution.ManifestService
	repositoryName  reference.Named
	authChallenger  authChallenger
}

var _ distribution.ManifestService = &proxyManifestStore{}

func (pms proxyManifestStore) Exists(ctx context.Context, dgst digest.Digest) (bool, error) {
	exists, err := pms.localManifests.Exists(ctx, dgst)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	if err := pms.authChallenger.tryEstablishChallenges(ctx); err != nil {
		return false, err
	}
	return pms.remoteManifests.Exists(ctx, dgst)
}

func (pms proxyManifestStore) Get(ctx context.Context, dgst digest.Digest, options ...distribution.ManifestServiceOption) (distribution.Manifest, error) {
	// At this point `dgst` was either specified explicitly, or returned by the
	// tagstore with the most recent association.
	var fromRemote bool
	manifest, err := pms.localManifests.Get(ctx, dgst, options...)
	if err != nil {
		if err := pms.authChallenger.tryEstablishChallenges(ctx); err != nil {
			return nil, err
		}

		manifest, err = pms.remoteManifests.Get(ctx, dgst, options...)
		if err != nil {
			return nil, err
		}
		fromRemote = true
	}

	_, payload, err := manifest.Payload()
	if err != nil {
		return nil, err
	}

	proxyMetrics.ManifestPush(uint64(len(payload)))
	if fromRemote {
		proxyMetrics.ManifestPull(uint64(len(payload)))

		_, err = pms.localManifests.Put(ctx, manifest)
		if err != nil {
			return nil, err
		}
	}

	return manifest, err
}

func (pms proxyManifestStore) Put(ctx context.Context, manifest distribution.Manifest, options ...distribution.ManifestServiceOption) (digest.Digest, error) {
	return pms.localManifests.Put(ctx, manifest, options...)
}

func (pms proxyManifestStore) Delete(ctx context.Context, dgst digest.Digest) error {
	return pms.localManifests.Delete(ctx, dgst)
}
