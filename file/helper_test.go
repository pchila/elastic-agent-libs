// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build !integration
// +build !integration

package file

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSafeFileRotateExistingFile(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "")
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, os.RemoveAll(tempdir))
	}()

	// create an existing registry file
	err = ioutil.WriteFile(filepath.Join(tempdir, "registry"),
		[]byte("existing filebeat"), 0x777)
	assert.NoError(t, err)

	// create a new registry.new file
	err = ioutil.WriteFile(filepath.Join(tempdir, "registry.new"),
		[]byte("new filebeat"), 0x777)
	assert.NoError(t, err)

	// rotate registry.new into registry
	err = SafeFileRotate(filepath.Join(tempdir, "registry"),
		filepath.Join(tempdir, "registry.new"))
	assert.NoError(t, err)

	contents, err := ioutil.ReadFile(filepath.Join(tempdir, "registry"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("new filebeat"), contents)

	// do it again to make sure we deal with deleting the old file

	err = ioutil.WriteFile(filepath.Join(tempdir, "registry.new"),
		[]byte("new filebeat 1"), 0x777)
	assert.NoError(t, err)

	err = SafeFileRotate(filepath.Join(tempdir, "registry"),
		filepath.Join(tempdir, "registry.new"))
	assert.NoError(t, err)

	contents, err = ioutil.ReadFile(filepath.Join(tempdir, "registry"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("new filebeat 1"), contents)

	// and again for good measure

	err = ioutil.WriteFile(filepath.Join(tempdir, "registry.new"),
		[]byte("new filebeat 2"), 0x777)
	assert.NoError(t, err)

	err = SafeFileRotate(filepath.Join(tempdir, "registry"),
		filepath.Join(tempdir, "registry.new"))
	assert.NoError(t, err)

	contents, err = ioutil.ReadFile(filepath.Join(tempdir, "registry"))
	assert.NoError(t, err)
	assert.Equal(t, []byte("new filebeat 2"), contents)
}

func TestSafeFileRotateSymLinks(t *testing.T) {
	tempdir := t.TempDir()

	// create existing target and link
	exTarget := "existing_target"
	exTargetAbsPath := filepath.Join(tempdir, exTarget)
	f, err := os.Create(exTargetAbsPath)
	require.NoError(t, err)

	err = f.Close()
	require.NoError(t, err)

	exLink := "existing_link"
	linkAbsPath := filepath.Join(tempdir, exLink)
	err = os.Symlink(exTargetAbsPath, linkAbsPath)
	require.NoError(t, err)

	//create new target and temp link
	newTarget := "new_target"
	newTargetAbsPath := filepath.Join(tempdir, newTarget)

	f, err = os.Create(newTargetAbsPath)
	require.NoError(t, err)

	err = f.Close()
	require.NoError(t, err)

	tmpLink := "new_link"
	tmpLinkAbsPath := filepath.Join(tempdir, tmpLink)
	err = os.Symlink(newTargetAbsPath, tmpLinkAbsPath)
	require.NoError(t, err)

	// rotate symlinks
	err = SafeFileRotate(linkAbsPath, tmpLinkAbsPath)
	assert.NoError(t, err)

	require.FileExistsf(t, linkAbsPath, "existing link %q does not exist", linkAbsPath)
	assert.NoFileExistsf(t, tmpLinkAbsPath, "the tmp link with new target %q was not deleted", tmpLinkAbsPath)

	actualLinkTarget, err := os.Readlink(linkAbsPath)
	assert.NoError(t, err)

	assert.Equal(t, newTargetAbsPath, actualLinkTarget)
}
