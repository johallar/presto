/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React, { useState, useEffect, useRef, useCallback } from "react";

import {addExponentiallyWeightedToHistory, addToHistory, formatCount, formatDataSizeBytes, precisionRound} from "../utils";

const SPARKLINE_PROPERTIES = {
    width: '100%',
    height: '75px',
    fillColor: '#3F4552',
    lineColor: '#747F96',
    spotColor: '#1EDCFF',
    tooltipClassname: 'sparkline-tooltip',
    disableHiddenCheck: true,
};

export const ClusterHUD = () => {
    const [runningQueries, setRunningQueries] = useState([]);
    const [queuedQueries, setQueuedQueries] = useState([]);
    const [blockedQueries, setBlockedQueries] = useState([]);
    const [activeWorkers, setActiveWorkers] = useState([]);
    const [runningDrivers, setRunningDrivers] = useState([]);
    const [reservedMemory, setReservedMemory] = useState([]);
    const [rowInputRate, setRowInputRate] = useState([]);
    const [byteInputRate, setByteInputRate] = useState([]);
    const [perWorkerCpuTimeRate, setPerWorkerCpuTimeRate] = useState([]);

    const [lastRender, setLastRender] = useState(null);
    const [lastRefresh, setLastRefresh] = useState(null);

    const [lastInputRows, setLastInputRows] = useState(null);
    const [lastInputBytes, setLastInputBytes] = useState(null);
    const [lastCpuTime, setLastCpuTime] = useState(null);


    const timeoutIdRef = useRef(null);

    const resetTimer = useCallback(() => {
        clearTimeout(timeoutIdRef.current);
        // stop refreshing when query finishes or fails
        // Note: query and ended properties don't exist in this component, so this condition is always true
        timeoutIdRef.current = setTimeout(() => {
            refreshLoop();
        }, 1000);
    }, []);

    const refreshLoop = useCallback(() => {
        clearTimeout(timeoutIdRef.current); // to stop multiple series of refreshLoop from going on simultaneously
        $.get('/v1/cluster', function (clusterState) {

            let newRowInputRate = [];
            let newByteInputRate = [];
            let newPerWorkerCpuTimeRate = [];
            if (lastRefresh !== null) {
                const rowsInputSinceRefresh = clusterState.totalInputRows - lastInputRows;
                const bytesInputSinceRefresh = clusterState.totalInputBytes - lastInputBytes;
                const cpuTimeSinceRefresh = clusterState.totalCpuTimeSecs - lastCpuTime;
                const secsSinceRefresh = (Date.now() - lastRefresh) / 1000.0;

                newRowInputRate = addExponentiallyWeightedToHistory(rowsInputSinceRefresh / secsSinceRefresh, rowInputRate);
                newByteInputRate = addExponentiallyWeightedToHistory(bytesInputSinceRefresh / secsSinceRefresh, byteInputRate);
                newPerWorkerCpuTimeRate = addExponentiallyWeightedToHistory((cpuTimeSinceRefresh / clusterState.activeWorkers) / secsSinceRefresh, perWorkerCpuTimeRate);
            }

            // Update all state variables
            setRunningQueries(prev => addToHistory(clusterState.runningQueries, prev));
            setQueuedQueries(prev => addToHistory(clusterState.queuedQueries, prev));
            setBlockedQueries(prev => addToHistory(clusterState.blockedQueries, prev));
            setActiveWorkers(prev => addToHistory(clusterState.activeWorkers, prev));

            setRunningDrivers(prev => addExponentiallyWeightedToHistory(clusterState.runningDrivers, prev));
            setReservedMemory(prev => addExponentiallyWeightedToHistory(clusterState.reservedMemory, prev));

            setRowInputRate(newRowInputRate);
            setByteInputRate(newByteInputRate);
            setPerWorkerCpuTimeRate(newPerWorkerCpuTimeRate);

            setLastInputRows(clusterState.totalInputRows);
            setLastInputBytes(clusterState.totalInputBytes);
            setLastCpuTime(clusterState.totalCpuTimeSecs);

            setLastRefresh(Date.now());

            resetTimer();
        })
        .fail(function () {
            resetTimer();
        });
    }, [lastRefresh, lastInputRows, lastInputBytes, lastCpuTime, rowInputRate, byteInputRate, perWorkerCpuTimeRate, resetTimer]);

    // Mount effect - replaces componentDidMount
    useEffect(() => {
        refreshLoop();
        
        // Cleanup on unmount
        return () => {
            clearTimeout(timeoutIdRef.current);
        };
    }, []);

    // Update effect - replaces componentDidUpdate
    useEffect(() => {
        // prevent multiple calls to componentDidUpdate (resulting from calls to setState or otherwise) within the refresh interval from re-rendering sparklines/charts
        if (lastRender === null || (Date.now() - lastRender) >= 1000) {
            const renderTimestamp = Date.now();
            $('#running-queries-sparkline').sparkline(runningQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#blocked-queries-sparkline').sparkline(blockedQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#queued-queries-sparkline').sparkline(queuedQueries, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));

            $('#active-workers-sparkline').sparkline(activeWorkers, $.extend({}, SPARKLINE_PROPERTIES, {chartRangeMin: 0}));
            $('#running-drivers-sparkline').sparkline(runningDrivers, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: precisionRound}));
            $('#reserved-memory-sparkline').sparkline(reservedMemory, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: formatDataSizeBytes}));

            $('#row-input-rate-sparkline').sparkline(rowInputRate, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: formatCount}));
            $('#byte-input-rate-sparkline').sparkline(byteInputRate, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: formatDataSizeBytes}));
            $('#cpu-time-rate-sparkline').sparkline(perWorkerCpuTimeRate, $.extend({}, SPARKLINE_PROPERTIES, {numberFormatter: precisionRound}));

            setLastRender(renderTimestamp);
        }

        $('[data-bs-toggle="tooltip"]')?.tooltip?.();
    }, [runningQueries, queuedQueries, blockedQueries, activeWorkers, runningDrivers, reservedMemory, rowInputRate, byteInputRate, perWorkerCpuTimeRate, lastRender]);

    return (<div className="row">
        <div className="col-12">
            <div className="row">
                <div className="col-4">
                    <div className="stat-title">
                        <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Total number of queries currently running">
                            Running queries
                        </span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat-title">
                        <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Total number of active worker nodes">
                            Active workers
                        </span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat-title">
                        <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Moving average of input rows processed per second">
                            Rows/sec
                        </span>
                    </div>
                </div>
            </div>
            <div className="row stat-line-end">
                <div className="col-4">
                    <div className="stat stat-large">
                        <span className="stat-text">
                            {runningQueries[runningQueries.length - 1]}
                        </span>
                        <span className="sparkline" id="running-queries-sparkline"><div className="loader">Loading ...</div></span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat stat-large">
                        <span className="stat-text">
                            {activeWorkers[activeWorkers.length - 1]}
                        </span>
                        <span className="sparkline" id="active-workers-sparkline"><div className="loader">Loading ...</div></span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat stat-large">
                        <span className="stat-text">
                            {formatCount(rowInputRate[rowInputRate.length - 1])}
                        </span>
                        <span className="sparkline" id="row-input-rate-sparkline"><div className="loader">Loading ...</div></span>
                    </div>
                </div>
            </div>
            <div className="row">
                <div className="col-4">
                    <div className="stat-title">
                        <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Total number of queries currently queued and awaiting execution">
                            Queued queries
                        </span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat-title">
                        <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Moving average of total running drivers">
                            Runnable drivers
                        </span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat-title">
                        <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Moving average of input bytes processed per second">
                            Bytes/sec
                        </span>
                    </div>
                </div>
            </div>
            <div className="row stat-line-end">
                <div className="col-4">
                    <div className="stat stat-large">
                        <span className="stat-text">
                            {queuedQueries[queuedQueries.length - 1]}
                        </span>
                        <span className="sparkline" id="queued-queries-sparkline"><div className="loader">Loading ...</div></span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat stat-large">
                        <span className="stat-text">
                            {formatCount(runningDrivers[runningDrivers.length - 1])}
                        </span>
                        <span className="sparkline" id="running-drivers-sparkline"><div className="loader">Loading ...</div></span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat stat-large">
                        <span className="stat-text">
                            {formatDataSizeBytes(byteInputRate[byteInputRate.length - 1])}
                        </span>
                        <span className="sparkline" id="byte-input-rate-sparkline"><div className="loader">Loading ...</div></span>
                    </div>
                </div>
            </div>
            <div className="row">
                <div className="col-4">
                    <div className="stat-title">
                        <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Total number of queries currently blocked and unable to make progress">
                            Blocked Queries
                        </span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat-title">
                        <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Total amount of memory reserved by all running queries">
                            Reserved Memory (B)
                        </span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat-title">
                        <span className="text" data-bs-toggle="tooltip" data-placement="right" title="Moving average of CPU time utilized per second per worker">
                            Worker Parallelism
                        </span>
                    </div>
                </div>
            </div>
            <div className="row stat-line-end">
                <div className="col-4">
                    <div className="stat stat-large">
                        <span className="stat-text">
                            {blockedQueries[blockedQueries.length - 1]}
                        </span>
                        <span className="sparkline" id="blocked-queries-sparkline"><div className="loader">Loading ...</div></span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat stat-large">
                        <span className="stat-text">
                            {formatDataSizeBytes(reservedMemory[reservedMemory.length - 1])}
                        </span>
                        <span className="sparkline" id="reserved-memory-sparkline"><div className="loader">Loading ...</div></span>
                    </div>
                </div>
                <div className="col-4">
                    <div className="stat stat-large">
                        <span className="stat-text">
                            {formatCount(perWorkerCpuTimeRate[perWorkerCpuTimeRate.length - 1])}
                        </span>
                        <span className="sparkline" id="cpu-time-rate-sparkline"><div className="loader">Loading ...</div></span>
                    </div>
                </div>
            </div>
        </div>
    </div>);
};

export default ClusterHUD;