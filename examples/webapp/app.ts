// tsc --lib dom,es2015 app.ts --outfile app.js
const start_tx_url = "/transactions";
const create_blob_url = "/blob";

var mail_from_display =
    document.getElementById("mail_from_display") as HTMLInputElement;
var mail_from = document.getElementById("mail_from") as HTMLInputElement;

mail_from.value = "alice@sandbox.gloop.org";

var rcpt_to_display =
    document.getElementById("rcpt_to_display") as HTMLInputElement;
var rcpt_to = document.getElementById("rcpt_to") as HTMLInputElement;

rcpt_to.value = "bucy@sandbox.gloop.org";

var subject = document.getElementById("subject") as HTMLInputElement;

var start = document.getElementById("start") as HTMLInputElement;

var related = document.getElementById("related") as HTMLInputElement;
var related_uri : string[] = [];
var attachment = document.getElementById("attachments") as HTMLInputElement;
var attachment_uri = [];

function log(x) {
    const log = document.getElementById("log");
    var div = document.createElement("div")
    div.textContent += x;
    log.appendChild(div);
}

var tx_url = null;
var blob_url = null;
var html_body_url = null;
var body_field = null;

start.onclick = (function() {
    var xhr = new XMLHttpRequest();
    xhr.open("POST", start_tx_url);
    xhr.onreadystatechange = function() {
	if (xhr.readyState != 4) { return; }
	const tx = JSON.parse(xhr.response);
        if (xhr.status != 201) {
            log("POST tx failed");
            return;
        }
        log("POST tx done");
        tx_url = xhr.getResponseHeader("location")
        send_body(tx);
    }

    xhr.setRequestHeader("Content-Type", "application/json");
    xhr.send(JSON.stringify({"http_host": "msa-output",
                             "mail_from": {"m": mail_from.value},
                             "rcpt_to": [{"m": rcpt_to.value}]}));
    log("POST tx sent");
});

function send_body(tx) {
    var file = null;
    for (const input_id of ["body", "body_html"]) {
        const input = document.getElementById(input_id) as HTMLInputElement;
        if (input == null || input.files == null) {
            continue;
        }
        const file_list = input.files as FileList;
        if (file_list.length == 0) {
            continue;
        }
        file = file_list[0] as File;
        body_field = input_id;
        break;
    }
    if (file == null) {
        log("no body file");
        return;
    }
    var xhr = new XMLHttpRequest();
    xhr.open("POST", create_blob_url);

    xhr.onreadystatechange = function() {
	if (xhr.readyState != 4) { return; }
        if (xhr.status != 201) {
            log("POST blob failed");
            return;
        }
        log("POST blob done");
        blob_url = xhr.getResponseHeader("location");
        if (body_field == "body") {
            update_tx();
        }
    }

    xhr.send(file)
    log("POST blob sent " + body_field);
    if (body_field == "body_html") {
        send_related();
    }
}

function send_related() {
    const input = document.getElementById("related") as HTMLInputElement;
    if (input == null || input.files == null) {
        return;
    }
    const file_list = input.files as FileList;
    const file_list_length : number = file_list.length;
    for (var i = 0; i < file_list_length; ++i) {
        var file = file_list[i] as File
        var xhr = new XMLHttpRequest();
        xhr.open("POST", create_blob_url);
        related_uri.push(null);
        xhr.onreadystatechange = function(
            xhr : XMLHttpRequest, ii : number, total : number) {
            console.log("related on ready " + ii + " " + total)
	    if (xhr.readyState != 4) { return; }
            if (xhr.status != 201) {
                log("POST blob failed");
                return;
            }
            log("POST blob done");
            related_uri[ii] = xhr.getResponseHeader("location");
            var done = true;
            for (var j = 0; j < total; ++j) {
                console.log("related sent " + ii + " " + j + " " +
                            related_uri[j])
                if (related_uri[j] == null) {
                    done = false;
                    break;
                }
            }
            if (!done) {
                return;
            }
            update_tx();
        }.bind(null, xhr, i, file_list_length)
        xhr.send(file)
        log("POST blob sent " + file.name + " " + file.type);
    }
}

function add_related(payload) {
    if (related_uri.length == 0) {
        return;
    }

    const input = document.getElementById("related") as HTMLInputElement;
    if (input == null || input.files == null) {
        throw new Error("null input");
    }
    const file_list = input.files as FileList;
    if (file_list.length != related_uri.length) {
        throw new Error("input mismatch");
    }

    var att = []
    for (const i in related_uri) {
        var file = file_list[i] as File;
        att.push({
            "content_type": file.type,
            "content_uri": related_uri[i],
            "content_id": file.name
        })
    }
    payload["related_attachments"] = att
}

function update_tx() {
    var xhr = new XMLHttpRequest();
    xhr.open("PATCH", tx_url);
    xhr.onreadystatechange = function() {
	if (xhr.readyState != 4) { return; }
        if (xhr.status >= 300) {
            log("PATCH tx failed");
            return;
        }
        log("PATCH tx done");
    }

    var json = {}
    if (body_field == "body") {
        json["body"] = blob_url;
    } else if (body_field == "body_html") {
        var payload = {
            "headers": [
                ["from", [{"display_name": mail_from_display.value,
                           "address": mail_from.value}]],
                ["to", [{"display_name": rcpt_to_display.value,
                         "address": rcpt_to.value}]],
                ["subject", subject.value],
            ],
            "text_body": [
                {
                    "content_type": "text/html",
                    "content_uri": blob_url
                }
            ]
        }
        add_related(payload)

        json["message_builder"] = payload;
    }

    xhr.setRequestHeader("Content-Type", "application/json");
    xhr.send(JSON.stringify(json));
    log("PATCH tx sent");
}
