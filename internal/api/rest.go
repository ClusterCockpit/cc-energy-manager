// Copyright (C) 2023 NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package api

import (
	"fmt"
	"strings"
)

func ApiPrinter(name string) error {
	// case insensitive string comparison
	if strings.EqualFold(name, "david") {
		fmt.Printf("Hello %s\n", name)
	} else {
		return fmt.Errorf("wrong name %s", name)
	}

	return nil
}
