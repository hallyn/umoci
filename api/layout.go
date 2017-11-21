package umoci

import (
	"github.com/openSUSE/umoci/oci/cas/dir"
	"github.com/openSUSE/umoci/oci/casext"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)


type Layout struct {
	Dir	string
	Tags	[]string
}

func OpenLayout(path string) (*Layout, error) {
	layout := Layout{Dir: path}

	// Get a reference to the CAS.
	engine, err := dir.Open(path)
	if err != nil {
		return nil, errors.Wrap(err, "open CAS")
	}
	engineExt := casext.NewEngine(engine)
	defer engine.Close()

	names, err := engineExt.ListReferences(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "list references")
	}

	layout.Tags = names
	return &layout, nil
}
