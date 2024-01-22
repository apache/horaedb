/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cmd

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/apache/incubator-horaedb/ctl/operation"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "horaectl",
	Short: "horaectl is a command line tool for HoraeDB",
	Run:   func(cmd *cobra.Command, args []string) {},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}

	for _, arg := range os.Args {
		if arg == "-h" || arg == "--help" {
			os.Exit(0)
		}
	}

	for {
		printPrompt(viper.GetString(operation.RootMetaAddr), viper.GetString(operation.RootCluster))
		err = ReadArgs(os.Stdin)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if err = rootCmd.Execute(); err != nil {
			fmt.Println(err)
			os.Args = []string{}
		}
	}
}

func init() {
	rootCmd.PersistentFlags().String(operation.RootMetaAddr, "127.0.0.1:8080", "meta addr is used to connect to meta server")
	viper.BindPFlag(operation.RootMetaAddr, rootCmd.PersistentFlags().Lookup(operation.RootMetaAddr))

	rootCmd.PersistentFlags().StringP(operation.RootCluster, "c", "defaultCluster", "")
	viper.BindPFlag(operation.RootCluster, rootCmd.PersistentFlags().Lookup(operation.RootCluster))

	rootCmd.CompletionOptions = cobra.CompletionOptions{
		DisableDefaultCmd:   true,
		DisableNoDescFlag:   true,
		DisableDescriptions: true,
		HiddenDefaultCmd:    true,
	}
}

func printPrompt(address, cluster string) {
	fmt.Printf("%s(%s) > ", address, cluster)
}

// ReadArgs Forked from https://github.com/apache/incubator-seata-ctl/blob/8427314e04cdc435b925ed41573b37e3addeea34/action/common/args.go#L29
func ReadArgs(in io.Reader) error {
	os.Args = []string{""}

	scanner := bufio.NewScanner(in)

	var lines []string

	for scanner.Scan() {
		line := strings.Trim(scanner.Text(), "\r\n ")
		if line == "" {
			return nil
		}
		if line[len(line)-1] == '\\' {
			line = line[:len(line)-1]
			lines = append(lines, line)
		} else {
			lines = append(lines, line)
			break
		}
	}

	argsStr := strings.Join(lines, " ")
	rawArgs := strings.Split(argsStr, "'")

	if len(rawArgs) != 1 && len(rawArgs) != 3 {
		return errors.New("read args from input error")
	}

	args := strings.Split(rawArgs[0], " ")

	if len(rawArgs) == 3 {
		args = append(args, rawArgs[1])
		args = append(args, strings.Split(rawArgs[2], " ")...)
	}

	for _, arg := range args {
		if arg != "" {
			os.Args = append(os.Args, strings.TrimSpace(arg))
		}
	}
	return nil
}
