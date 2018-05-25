import * as AWS from "aws-sdk";
import fetch from "node-fetch";
import {  assoc, assocPath, contains, filter, pipe } from "ramda";
import {  bindCallback, empty, from, Observable, of as rxOf, Subject } from "rxjs";
import { catchError, concatMap, delay, filter as rxFilter, map, tap } from "rxjs/operators";
import * as sourceMapSupport from "source-map-support";
import * as url from "url";
sourceMapSupport.install();

AWS.config.update({
    region: "us-east-1",
});

const docClient = new AWS.DynamoDB.DocumentClient();

const dynamoDbGet = bindCallback<AWS.DynamoDB.DocumentClient.GetItemInput, AWS.AWSError, AWS.DynamoDB.DocumentClient.GetItemOutput>(
    docClient.get.bind(docClient) as (
        params: AWS.DynamoDB.DocumentClient.GetItemInput, callback?: (err: AWS.AWSError, data: AWS.DynamoDB.DocumentClient.GetItemOutput) => void,
    ) => AWS.Request<AWS.DynamoDB.DocumentClient.GetItemOutput, AWS.AWSError>,
);

const dynamoDbUpdate = bindCallback<AWS.DynamoDB.DocumentClient.UpdateItemInput, AWS.AWSError, AWS.DynamoDB.DocumentClient.UpdateItemOutput>(
    docClient.update.bind(docClient) as (
        params: AWS.DynamoDB.DocumentClient.UpdateItemInput, callback?: (err: AWS.AWSError, data: AWS.DynamoDB.DocumentClient.UpdateItemOutput) => void,
    ) => AWS.Request<AWS.DynamoDB.DocumentClient.UpdateItemOutput, AWS.AWSError>,
);

process.env.AIRTABLE_KEY = "keyhbcvvQIT6NC7iZ";
process.env.MONZO_ACCESS_TOKEN = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJlYiI6IkVSVmVUZ0hYdXhlaXhtM2VGOVY2IiwianRpIjoiYWNjdG9rXzAwMDA5V013SzFMTTVMNEtKVEZkWW4iLCJ0eXAiOiJhdCIsInYiOiI1In0.Zohx_Lkgl7D9PFuLcoaRWvYHVP4DLWjiLJlBqE_S4aq93r87XCZX1C0Lxq0b-YiS0OK-hITN5Qmm14DddW9yow";
process.env.MONZO_POT_ID = "pot_00009WMwS3jh4cRM3AT6dV";
process.env.MONZO_ACCOUNT_ID = "acc_00009TpT6FZ50nhiitZVwX";
process.env.MONZO_CLIENT_ID = "oauth2client_00009WZc8MjBHSgC3la6Yz";
process.env.MONZO_CLIENT_SECRET = "mnzconf.n44Bo+He2bE1KsU3EbLqthJSAcH8rvjPc5ZDBsR8yYmX1gDA/jdFvACm/62dx8W8JcfcVFX1apeF8gTEhUF0";
process.env.PUSHOVER_TOKEN = "a72nyvga1h2q4tf4pr1dfz12r7qjo9";
process.env.PUSHOVER_USER = "u2snmzbc3u6fkszekrjnftrqk1pwch";
process.env.PUSHOVER_DEVICE = "sm-g900f";

interface IMonzoPipelinePayload {
    row: IAirtableRow;
    monzoRefreshToken: string;
    monzoAccessToken: string;
}

interface IAirtableResponse {
    records: IAirtableRow[];
}

interface IAirtableRow {
    "id": string;
    "fields": {
        "Next unit of work": undefined | string;
        "What slowed you down?": undefined | string;
        "Created at": undefined | string;
        "Completed at": undefined | string;
        "Reward": undefined | string;
        "Processed": undefined | string;
    };
}

const fetchUnprocessedAirtableRows = () => {
    return from(
        fetch("https://api.airtable.com/v0/appvcDcxH8llgwoN7/Workflow?&view=Grid%20view&filterByFormula=NOT(Processed = 1)", {
            headers: {
                Authorization: "Bearer " + process.env.AIRTABLE_KEY,
            },
        }).then((res) => {
            console.log("fetchUnprocessedAirtableRows GET response", res);
            return res.json();
        }).then((json) => {
            console.log("fetchUnprocessedAirtableRows GET json", json);
            return json;
        }),
    );
};

const isRowReadyForProcessing = (row: IAirtableRow): boolean => {
    const isReady = (row.fields["What slowed you down?"]) ? true : false;

    if (! isReady) {
        console.log("Filtered out row as not ready for processing:", row);
    }

    return isReady;
};

const generateReward = (row: IAirtableRow) => {
    const rand = Math.random();
    const cutoff = 0.15;
    let reward = 0;
    if (rand < cutoff) {
        reward = Math.round(Math.random() * 400);
    }
    console.log("Generated reward.", "Row id:", row.id, reward, "Probability", cutoff);
    return reward;
};

const addRewardToAirtableRow = (row: IAirtableRow): IAirtableRow => {
    return pipe(
        assocPath(
            ["fields", "Reward"],
            generateReward(row),
        ),
        assocPath(
            ["fields", "Completed at"],
            (new Date()).toISOString(),
        ),
    )(row) as IAirtableRow;
};

const getMonzoRefreshToken = (row: IAirtableRow): Observable<IMonzoPipelinePayload> => {
    return dynamoDbGet({
        Key: {
            user_id: 1,
        },
        TableName: "MonzoRefreshToken",
    }).pipe(
        map(([err, res]): IMonzoPipelinePayload => {
            if (err) {
                console.error("dyanmodb GET error", err, "row", row);
                throw new Error("dynamodb GET error");
            }

            if (! res.Item.refresh_token || typeof res.Item.refresh_token !== "string") {
                console.error("dynamodb invalid GET response", res, "row:", row);
                throw new Error("dynamodb invalid GET response");
            }

            console.log("dynamodb GET success", res);

            return {
                monzoAccessToken: "",
                monzoRefreshToken: res.Item.refresh_token,
                row,
            };
        }),
    );
};

const getMonzoAccessToken = (monzoPipelinePayload: IMonzoPipelinePayload) => {
    const formData = new url.URLSearchParams();
    formData.append("grant_type", "refresh_token");
    formData.append("client_id", process.env.MONZO_CLIENT_ID);
    formData.append("client_secret", process.env.MONZO_CLIENT_SECRET);
    formData.append("refresh_token", monzoPipelinePayload.monzoRefreshToken);
    return from(
        fetch("https://api.monzo.com/oauth2/token", {
            body: formData as BodyInit,
            method: "POST",
        }).then((res) => {
            console.log("getMonzoAccessToken response", res);
            return res.json();
        }),
    ).pipe(
        map((json) => {
            console.log("getMonzoAccessToken json", json);
            return pipe(
                assoc("monzoRefreshToken", json.refresh_token),
                assoc("monzoAccessToken", json.access_token),
            )(monzoPipelinePayload);
        }),
    );
};

const updateRefreshTokenInDynamoDb = (monzoPipelinePayload: IMonzoPipelinePayload) => {
    return dynamoDbUpdate({
        ExpressionAttributeValues: {
            ":t": monzoPipelinePayload.monzoRefreshToken,
        },
        Key: {
            user_id: 1,
        },
        ReturnValues: "UPDATED_NEW",
        TableName: "MonzoRefreshToken",
        UpdateExpression: "set refresh_token = :t",
    }).pipe(
        map(([err, res]): IMonzoPipelinePayload => {
            if (err) {
                console.error("dyanmodb UPDATE error", err, "row", monzoPipelinePayload.row);
                throw new Error("dynamodb UPDATE error");
            }

            console.log("dynamodb UPDATE success", res);

            return monzoPipelinePayload;
        }),
    );
};

const doMonzoTransaction = (payload: IMonzoPipelinePayload) => {
    const formData = new url.URLSearchParams();
    formData.append("amount", payload.row.fields.Reward);
    formData.append("destination_account_id", process.env.MONZO_ACCOUNT_ID);
    formData.append("dedupe_id", payload.row.id);
    return from(
        fetch("https://api.monzo.com/pots/" + process.env.MONZO_POT_ID + "/withdraw", {
            body: formData as BodyInit,
            headers: {
                Authorization: `Bearer ${payload.monzoAccessToken}`,
            },
            method: "PUT",
        })
        .then((res) => {
            console.log("doMonzoTransaction PUT response", res);
            return res.json();
        })
        .then((json) => {
            console.log("doMonzoTransaction PUT json", json);
        }),
    ).pipe(
        map(() => payload),
    );
};

const doPushoverNotification = (payload: IMonzoPipelinePayload) => {
    const formData = new url.URLSearchParams();
    formData.append("token", process.env.PUSHOVER_TOKEN);
    formData.append("user", process.env.PUSHOVER_USER);
    formData.append("device", process.env.PUSHOVER_DEVICE);
    formData.append("title", `${payload.row.fields["Next unit of work"]}' - Completed!`);
    formData.append("message", `You get £${(Number(payload.row.fields.Reward) / 100).toFixed(2)}!`);
    return from(
        fetch("https://api.pushover.net/1/messages.json", {
            body: formData as BodyInit,
            method: "POST",
        }).then((res) => {
            console.log("doPushoverNotification POST response", res);
            return res.json();
        }).then((json) => {
            console.log("doPushoverNotification POST json", json);
            return json;
        }),
    ).pipe(
        map(() => payload),
    );
};

const markRowAsCompletedInAirtable = (payload: IMonzoPipelinePayload) => {
    return from(
        fetch("https://api.airtable.com/v0/appvcDcxH8llgwoN7/Workflow/" + payload.row.id, {
            body: JSON.stringify({
                fields: {
                    "Completed at": payload.row.fields["Completed at"],
                    "Processed": true,
                    "Reward": Number(payload.row.fields.Reward) / 100,
                }}),
            headers: {
                "Authorization": `Bearer ${process.env.AIRTABLE_KEY}`,
                "Content-Type": "application/json",
            },
            method: "PATCH",
        }).then((res) => {
            console.log("markRowAsCompletedInAirtable PATCH response", res);
            return res.json();
        }).then((json) => {
            console.log("markRowAsCompletedInAirtable PATCH json", json);
            return json;
        }),
    ).pipe(
        map(() => payload),
    );
};

const trigger = new Subject();

trigger
.pipe(
    delay(1000),
    concatMap(fetchUnprocessedAirtableRows),
    tap((json: IAirtableResponse) => {
        console.log("Airtable row query received", json);
    }),
    concatMap((json: IAirtableResponse) => {
        const filtered = filter(isRowReadyForProcessing, json.records);
        if (filtered.length === 0) {
            trigger.next();
        }
        return filtered;
    }),
    map(addRewardToAirtableRow),
    concatMap((row): Observable<IMonzoPipelinePayload> => {
        if (row.fields.Reward) {
            return rxOf(row)
                .pipe(
                    concatMap(getMonzoRefreshToken),
                    concatMap(getMonzoAccessToken),
                    concatMap(updateRefreshTokenInDynamoDb),
                    concatMap(doMonzoTransaction),
                    concatMap(doPushoverNotification),
                );
        }
        return rxOf({row, monzoRefreshToken: "", monzoAccessToken: ""});
    }),
    concatMap(markRowAsCompletedInAirtable),
    catchError((ex) => {
        console.error("Caught generic exception", ex);
        trigger.next();
        return empty();
    }),
).subscribe(() => {
    console.log("trigger");
    trigger.next();
});

trigger.next();
