"use strict";
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const AWS = __importStar(require("aws-sdk"));
const node_fetch_1 = __importDefault(require("node-fetch"));
const ramda_1 = require("ramda");
const rxjs_1 = require("rxjs");
const operators_1 = require("rxjs/operators");
const sourceMapSupport = __importStar(require("source-map-support"));
const url = __importStar(require("url"));
sourceMapSupport.install();
AWS.config.update({
    region: "us-east-1",
});
const docClient = new AWS.DynamoDB.DocumentClient();
const dynamoDbGet = rxjs_1.bindCallback(docClient.get.bind(docClient));
const dynamoDbUpdate = rxjs_1.bindCallback(docClient.update.bind(docClient));
process.env.AIRTABLE_KEY = "keyhbcvvQIT6NC7iZ";
process.env.MONZO_ACCESS_TOKEN = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJlYiI6IkVSVmVUZ0hYdXhlaXhtM2VGOVY2IiwianRpIjoiYWNjdG9rXzAwMDA5V013SzFMTTVMNEtKVEZkWW4iLCJ0eXAiOiJhdCIsInYiOiI1In0.Zohx_Lkgl7D9PFuLcoaRWvYHVP4DLWjiLJlBqE_S4aq93r87XCZX1C0Lxq0b-YiS0OK-hITN5Qmm14DddW9yow";
process.env.MONZO_POT_ID = "pot_00009WMwS3jh4cRM3AT6dV";
process.env.MONZO_ACCOUNT_ID = "acc_00009TpT6FZ50nhiitZVwX";
process.env.MONZO_CLIENT_ID = "oauth2client_00009WZc8MjBHSgC3la6Yz";
process.env.MONZO_CLIENT_SECRET = "mnzconf.n44Bo+He2bE1KsU3EbLqthJSAcH8rvjPc5ZDBsR8yYmX1gDA/jdFvACm/62dx8W8JcfcVFX1apeF8gTEhUF0";
process.env.PUSHOVER_TOKEN = "a72nyvga1h2q4tf4pr1dfz12r7qjo9";
process.env.PUSHOVER_USER = "u2snmzbc3u6fkszekrjnftrqk1pwch";
process.env.PUSHOVER_DEVICE = "sm-g900f";
const fetchUnprocessedAirtableRows = () => {
    return rxjs_1.from(node_fetch_1.default("https://api.airtable.com/v0/appvcDcxH8llgwoN7/Workflow?&view=Grid%20view&filterByFormula=NOT(Processed = 1)", {
        headers: {
            Authorization: "Bearer " + process.env.AIRTABLE_KEY,
        },
    }).then((res) => {
        console.log("fetchUnprocessedAirtableRows GET response", res);
        return res.json();
    }).then((json) => {
        console.log("fetchUnprocessedAirtableRows GET json", json);
        return json;
    }));
};
const isRowReadyForProcessing = (row) => {
    const isReady = (row.fields["What slowed you down?"]) ? true : false;
    if (!isReady) {
        console.log("Filtered out row as not ready for processing:", row);
    }
    return isReady;
};
const generateReward = (row) => {
    const rand = Math.random();
    const cutoff = 0.15;
    let reward = 0;
    if (rand < cutoff) {
        reward = Math.round(Math.random() * 400);
    }
    console.log("Generated reward.", "Row id:", row.id, reward, "Probability", cutoff);
    return reward;
};
const addRewardToAirtableRow = (row) => {
    return ramda_1.pipe(ramda_1.assocPath(["fields", "Reward"], generateReward(row)), ramda_1.assocPath(["fields", "Completed at"], (new Date()).toISOString()))(row);
};
const getMonzoRefreshToken = (row) => {
    return dynamoDbGet({
        Key: {
            user_id: 1,
        },
        TableName: "MonzoRefreshToken",
    }).pipe(operators_1.map(([err, res]) => {
        if (err) {
            console.error("dyanmodb GET error", err, "row", row);
            throw new Error("dynamodb GET error");
        }
        if (!res.Item.refresh_token || typeof res.Item.refresh_token !== "string") {
            console.error("dynamodb invalid GET response", res, "row:", row);
            throw new Error("dynamodb invalid GET response");
        }
        console.log("dynamodb GET success", res);
        return {
            monzoAccessToken: "",
            monzoRefreshToken: res.Item.refresh_token,
            row,
        };
    }));
};
const getMonzoAccessToken = (monzoPipelinePayload) => {
    const formData = new url.URLSearchParams();
    formData.append("grant_type", "refresh_token");
    formData.append("client_id", process.env.MONZO_CLIENT_ID);
    formData.append("client_secret", process.env.MONZO_CLIENT_SECRET);
    formData.append("refresh_token", monzoPipelinePayload.monzoRefreshToken);
    return rxjs_1.from(node_fetch_1.default("https://api.monzo.com/oauth2/token", {
        body: formData,
        method: "POST",
    }).then((res) => {
        console.log("getMonzoAccessToken response", res);
        return res.json();
    })).pipe(operators_1.map((json) => {
        console.log("getMonzoAccessToken json", json);
        return ramda_1.pipe(ramda_1.assoc("monzoRefreshToken", json.refresh_token), ramda_1.assoc("monzoAccessToken", json.access_token))(monzoPipelinePayload);
    }));
};
const updateRefreshTokenInDynamoDb = (monzoPipelinePayload) => {
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
    }).pipe(operators_1.map(([err, res]) => {
        if (err) {
            console.error("dyanmodb UPDATE error", err, "row", monzoPipelinePayload.row);
            throw new Error("dynamodb UPDATE error");
        }
        console.log("dynamodb UPDATE success", res);
        return monzoPipelinePayload;
    }));
};
const doMonzoTransaction = (payload) => {
    const formData = new url.URLSearchParams();
    formData.append("amount", payload.row.fields.Reward);
    formData.append("destination_account_id", process.env.MONZO_ACCOUNT_ID);
    formData.append("dedupe_id", payload.row.id);
    return rxjs_1.from(node_fetch_1.default("https://api.monzo.com/pots/" + process.env.MONZO_POT_ID + "/withdraw", {
        body: formData,
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
    })).pipe(operators_1.map(() => payload));
};
const doPushoverNotification = (payload) => {
    const formData = new url.URLSearchParams();
    formData.append("token", process.env.PUSHOVER_TOKEN);
    formData.append("user", process.env.PUSHOVER_USER);
    formData.append("device", process.env.PUSHOVER_DEVICE);
    formData.append("title", `${payload.row.fields["Next unit of work"]}' - Completed!`);
    formData.append("message", `You get £${(Number(payload.row.fields.Reward) / 100).toFixed(2)}!`);
    return rxjs_1.from(node_fetch_1.default("https://api.pushover.net/1/messages.json", {
        body: formData,
        method: "POST",
    }).then((res) => {
        console.log("doPushoverNotification POST response", res);
        return res.json();
    }).then((json) => {
        console.log("doPushoverNotification POST json", json);
        return json;
    })).pipe(operators_1.map(() => payload));
};
const markRowAsCompletedInAirtable = (payload) => {
    return rxjs_1.from(node_fetch_1.default("https://api.airtable.com/v0/appvcDcxH8llgwoN7/Table%201/" + payload.row.id, {
        body: JSON.stringify({
            fields: {
                "Completed at": payload.row.fields["Completed at"],
                "Processed": true,
                "Reward": Number(payload.row.fields.Reward) / 100,
            }
        }),
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
    })).pipe(operators_1.map(() => payload));
};
const trigger = new rxjs_1.Subject();
trigger
    .pipe(operators_1.delay(1000), operators_1.concatMap(fetchUnprocessedAirtableRows), operators_1.tap((json) => {
    console.log("Airtable row query received", json);
}), operators_1.concatMap((json) => {
    const filtered = ramda_1.filter(isRowReadyForProcessing, json.records);
    if (filtered.length === 0) {
        trigger.next();
    }
    return filtered;
}), operators_1.map(addRewardToAirtableRow), operators_1.concatMap((row) => {
    if (row.fields.Reward) {
        return rxjs_1.of(row)
            .pipe(operators_1.concatMap(getMonzoRefreshToken), operators_1.concatMap(getMonzoAccessToken), operators_1.concatMap(updateRefreshTokenInDynamoDb), operators_1.concatMap(doMonzoTransaction), operators_1.concatMap(doPushoverNotification));
    }
    return rxjs_1.of({ row, monzoRefreshToken: "", monzoAccessToken: "" });
}), operators_1.concatMap(markRowAsCompletedInAirtable), operators_1.catchError((ex) => {
    console.error("Caught generic exception", ex);
    trigger.next();
    return rxjs_1.empty();
})).subscribe(() => {
    console.log("trigger");
    trigger.next();
});
trigger.next();
//# sourceMappingURL=workflow.js.map