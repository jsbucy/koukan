// tsc --lib dom,es2015 app.ts --outfile app.js
const start_tx_url = "/transactions";

var mail_from = document.getElementById("mail_from") as HTMLInputElement

mail_from.value = "alice@sandbox.gloop.org"

var rcpt_to = document.getElementById("rcpt_to") as HTMLInputElement

rcpt_to.value = "bucy@sandbox.gloop.org"


var start = document.getElementById("start") as HTMLInputElement

function log(x) {
    const log = document.getElementById("log");
    log.textContent += x;
}

start.onclick = (function() {
    var xhr = new XMLHttpRequest();
    xhr.open("POST", start_tx_url);
    xhr.onreadystatechange = function() {
	if (xhr.readyState != 4) { return; }
        log("POST done");
	const tx = JSON.parse(xhr.response)
        send_body(tx);
    }

    xhr.setRequestHeader("Content-Type", "application/json");
    xhr.send(JSON.stringify({"http_host": "msa-output",
                             "mail_from": {"m": mail_from.value},
                             "rcpt_to": [{"m": rcpt_to.value}],
                             "body": ""}));
    log("POST sent");
});

function send_body(tx) {
    const file_input = document.getElementById("body") as HTMLInputElement;
    const file_list = file_input.files as FileList;
    const file = file_list[0] as File;

    var xhr = new XMLHttpRequest();
    xhr.open("PUT", tx.body);

    xhr.onreadystatechange = function() {
	if (xhr.readyState != 4) { return; }
        log("PUT done");

    }

    xhr.send(file)
    log("PUT sent");
}
