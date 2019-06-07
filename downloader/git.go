package downloader

import (
	"context"
	"fmt"

	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

var (
	// ErrObjectTypeNotSupported returned by ResolveCommit when the
	// referenced object isn't a Commit nor a Tag.
	ErrObjectTypeNotSupported = errors.NewKind(
		"object type %q not supported")
)

const (
	cloneRootPath   = "local_repos"
	fetchHEADStr    = "+HEAD:refs/remotes/%s/HEAD"
	fetchRefSpecStr = "+refs/*:refs/remotes/%s/*"
)

func cloneRepo(
	fs billy.Filesystem,
	path, endpoint, id string,
) (*git.Repository, error) {
	repoFS, err := fs.Chroot(path)
	if err != nil {
		return nil, err
	}

	sto := filesystem.NewStorage(repoFS, cache.NewObjectLRUDefault())
	repo, err := git.Init(sto, nil)
	if err != nil {
		util.RemoveAll(fs, path)
		return nil, err
	}

	remote, err := createRemote(repo, id, endpoint)
	if err != nil {
		util.RemoveAll(fs, path)
		return nil, err
	}

	if err = remote.FetchContext(context.TODO(), &git.FetchOptions{
		RefSpecs: []config.RefSpec{
			config.RefSpec(fmt.Sprintf(fetchHEADStr, id)),
		},
		Force: true,
		Tags:  git.NoTags,
	}); err != nil {
		util.RemoveAll(fs, path)
		return nil, err
	}

	return repo, nil
}

func createRemote(r *git.Repository, id, endpoint string) (*git.Remote, error) {
	rc := &config.RemoteConfig{
		Name: id,
		URLs: []string{endpoint},
		Fetch: []config.RefSpec{
			config.RefSpec(fmt.Sprintf(fetchHEADStr, id)),
			config.RefSpec(fmt.Sprintf(fetchRefSpecStr, id)),
		}}

	remote, err := r.Remote(id)
	if err != nil {
		return r.CreateRemote(rc)
	}

	if remote.Config() == rc {
		return remote, nil
	}

	cfg, err := r.Config()
	if err != nil {
		return nil, err
	}

	cfg.Remotes[id] = rc
	if err := r.Storer.SetConfig(cfg); err != nil {
		return nil, err
	}

	return r.Remote(id)
}

func headCommit(repo *git.Repository, id string) (*object.Commit, error) {
	ref, err := repo.Reference(
		plumbing.NewRemoteHEADReferenceName(id),
		true,
	)

	if err != nil {
		return nil, err
	}

	return resolveCommit(repo, ref.Hash())
}

func resolveCommit(
	repo *git.Repository,
	hash plumbing.Hash,
) (*object.Commit, error) {
	obj, err := repo.Object(plumbing.AnyObject, hash)
	if err != nil {
		return nil, err
	}

	switch o := obj.(type) {
	case *object.Commit:
		return o, nil
	case *object.Tag:
		return resolveCommit(repo, o.Target)
	default:
		return nil, ErrObjectTypeNotSupported.New(o.Type())
	}
}

// rootCommitHash traverse the commit history from the given start commit
// following the first parent of each commit. The root commit found (commit
// with no parents) is returned.
func rootCommit(
	repo *git.Repository,
	start *object.Commit,
) (*object.Commit, error) {
	var (
		current = start
		err     error
	)

	for len(current.ParentHashes) > 0 {
		current, err = current.Parent(0)
		if err != nil {
			return nil, err
		}
	}

	return current, nil
}
