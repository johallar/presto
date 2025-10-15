import React from "react";
import lazy from "./lazy";
import { PageTitle } from "./components/PageTitle";

const ClusterHUD = lazy('ClusterHUD');
const QueryList = lazy('QueryList');

export const ClusterOverview = () => {
    return (
        <div>
            <h1>Cluster Overview</h1>
            <PageTitle titles={['Cluster Overview', 'Resource Groups', 'SQL Client']} urls={['./index.html', 'res_groups.html', 'sql_client.html']} current={0}/>
            <ClusterHUD />
            <QueryList />
        </div>
    );
};