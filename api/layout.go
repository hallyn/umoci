package umoci

import (
	"github.com/openSUSE/umoci/oci/cas"
	"github.com/openSUSE/umoci/oci/cas/dir"
	"github.com/openSUSE/umoci/oci/casext"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type Layout struct {
	Dir	string
	engine  cas.Engine
}

func (l *Layout) Close() {
	l.engine.Close()
}

func OpenLayout(path string) (*Layout, error) {
	layout := Layout{Dir: path}

	// Get a reference to the CAS.
	engine, err := dir.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open CAS")
	}
	layout.engine = engine

	return &layout, nil
}

func (l *Layout) ListTags() ([]string, error) {
	engineExt := casext.NewEngine(l.engine)

	return engineExt.ListReferences(context.Background())
}

func (l *Layout) AddTag(old, tag string) error {
	engineExt := casext.NewEngine(l.engine)
	descriptorPaths, err := engineExt.ResolveReference(context.Background(), old)
	if err != nil {
		return errors.Wrap(err, "get descriptor")
	}
	if len(descriptorPaths) == 0 {
		return errors.Errorf("tag not found: %s", old)
	}
	if len(descriptorPaths) != 1 {
		// TODO: Handle this more nicely.
		return errors.Errorf("tag is ambiguous: %s", old)
	}
	descriptor := descriptorPaths[0].Descriptor()

	descriptorPaths, err = engineExt.ResolveReference(context.Background(), tag)
	if err != nil {
		return errors.Wrap(err, "get descriptor")
	}
	if len(descriptorPaths) != 0 {
		return errors.Errorf("new tag already exists: %s", tag)
	}

	if err := engineExt.UpdateReference(context.Background(), tag, descriptor); err != nil {
		return errors.Wrap(err, "put reference")
	}

	return nil
}

func (l *Layout) RmTag(tag string) error {
	engineExt := casext.NewEngine(l.engine)

	if err := engineExt.DeleteReference(context.Background(), tag); err != nil {
		return errors.Wrap(err, "delete reference")
	}
	return nil
}
