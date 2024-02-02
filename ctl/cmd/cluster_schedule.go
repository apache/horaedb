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
	"github.com/apache/incubator-horaedb/ctl/operation"
	"github.com/spf13/cobra"
)

var enable bool

var scheduleCmd = &cobra.Command{
	Use:     "schedule",
	Aliases: []string{"s"},
	Short:   "Cluster schedule",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var getCmd = &cobra.Command{
	Use:   "get",
	Short: "Get the schedule status",
	Run: func(cmd *cobra.Command, args []string) {
		operation.ClusterScheduleGet()
	},
}

var setCmd = &cobra.Command{
	Use:   "set",
	Short: "Set the schedule status",
	Run: func(cmd *cobra.Command, args []string) {
		operation.ClusterScheduleSet(enable)
	},
}

func init() {
	setCmd.Flags().BoolVarP(&enable, "enable", "e", false, "enable or disable schedule")
	scheduleCmd.AddCommand(getCmd, setCmd)
	clusterCmd.AddCommand(scheduleCmd)
}
