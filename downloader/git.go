package downloader

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/src-d/go-borges"
	"github.com/src-d/go-borges/siva"
	"gopkg.in/src-d/go-billy.v4"
	"gopkg.in/src-d/go-billy.v4/util"
	"gopkg.in/src-d/go-errors.v1"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/cache"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	"gopkg.in/src-d/go-git.v4/plumbing/transport/http"
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

// CloneRepository clones a git repository from the given endpoint into the
// billy.Filesystem. A remote with the id is created for that.
func CloneRepository(
	ctx context.Context,
	fs billy.Filesystem,
	path, endpoint, id, token string,
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

	opts := &git.FetchOptions{
		RefSpecs: []config.RefSpec{
			config.RefSpec(fmt.Sprintf(fetchHEADStr, id)),
		},
		Force: true,
		Tags:  git.NoTags,
	}

	if token != "" {
		opts.Auth = &http.BasicAuth{
			Username: "gitcollector",
			Password: token,
		}
	}

	if err = remote.FetchContext(ctx, opts); err != nil {
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

// RootCommit traverse the commit history for the given remote following the
// first parent of each commit. The root commit found (commit with no parents)
// is returned.
func RootCommit(
	repo *git.Repository,
	remote string,
) (*object.Commit, error) {
	start, err := headCommit(repo, remote)
	if err != nil {
		return nil, err
	}

	current := start
	for len(current.ParentHashes) > 0 {
		current, err = current.Parent(0)
		if err != nil {
			return nil, err
		}
	}

	return current, nil
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

// PrepareRepository returns a borges.Repository ready to fetch changes.
// It creates a rooted repository copying the cloned repository in tmp to
// the siva file the library uses at the location with the given location ID,
// creating this location if not exists.
func PrepareRepository(
	ctx context.Context,
	lib *siva.Library,
	locID borges.LocationID,
	repoID borges.RepositoryID,
	endpoint string,
	tmp billy.Filesystem,
	clonePath string,
) (borges.Repository, error) {
	var r borges.Repository

	loc, err := lib.AddLocation(locID)
	if err != nil {
		if !siva.ErrLocationExists.Is(err) {
			return nil, err
		}

		loc, err = lib.Location(locID)
		if err != nil {
			return nil, err
		}

		r, err = loc.Get(repoID, borges.RWMode)
		if err != nil {
			r, err = loc.Init(repoID)
			if err != nil {
				return nil, err
			}
		}
	}

	if r == nil {
		r, err = createRootedRepo(ctx, loc, repoID, tmp, clonePath)
		if err != nil {
			return nil, err
		}
	}

	if _, err := createRemote(r.R(), repoID.String(), endpoint); err != nil {
		if cErr := r.Close(); cErr != nil {
			err = fmt.Errorf("%s: %s", err.Error(), cErr.Error())
		}

		return nil, err
	}

	return r, nil
}

// FetchChanges fetches changes for the given remote into the borges.Repository.
func FetchChanges(
	ctx context.Context,
	r borges.Repository,
	remote string,
	token string,
) error {
	opts := &git.FetchOptions{
		RemoteName: remote,
	}

	if token != "" {
		opts.Auth = &http.BasicAuth{
			Username: "gitcollector",
			Password: token,
		}
	}

	if err := r.R().FetchContext(
		ctx, opts,
	); err != nil && err != git.NoErrAlreadyUpToDate {
		if cErr := r.Close(); cErr != nil {
			err = fmt.Errorf("%s: %s", err.Error(), cErr.Error())
		}

		return err
	}

	return nil
}

func createRootedRepo(
	ctx context.Context,
	loc borges.Location,
	repoID borges.RepositoryID,
	clonedFS billy.Filesystem,
	clonedPath string,
) (borges.Repository, error) {
	repo, err := loc.Init(repoID)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		err = recursiveCopy(
			"/", repo.FS(),
			clonedPath, clonedFS,
		)

		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
		repo.Close()
		repo = nil
	}

	return repo, err
}

func recursiveCopy(
	dst string,
	dstFS billy.Filesystem,
	src string,
	srcFS billy.Filesystem,
) error {
	stat, err := srcFS.Stat(src)
	if err != nil {
		return err
	}

	if stat.IsDir() {
		err = dstFS.MkdirAll(dst, stat.Mode())
		if err != nil {
			return err
		}

		files, err := srcFS.ReadDir(src)
		if err != nil {
			return err
		}

		for _, file := range files {
			srcPath := filepath.Join(src, file.Name())
			dstPath := filepath.Join(dst, file.Name())

			err = recursiveCopy(dstPath, dstFS, srcPath, srcFS)
			if err != nil {
				return err
			}
		}
	} else {
		err = copyFile(dst, dstFS, src, srcFS, stat.Mode())
		if err != nil {
			return err
		}
	}

	return nil
}

func copyFile(
	dst string,
	dstFS billy.Filesystem,
	src string,
	srcFS billy.Filesystem,
	mode os.FileMode,
) error {
	_, err := srcFS.Stat(src)
	if err != nil {
		return err
	}

	fo, err := srcFS.Open(src)
	if err != nil {
		return err
	}
	defer fo.Close()

	fd, err := dstFS.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer fd.Close()

	_, err = io.Copy(fd, fo)
	if err != nil {
		fd.Close()
		dstFS.Remove(dst)
		return err
	}

	return nil
}
