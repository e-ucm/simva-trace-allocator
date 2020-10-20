var Minio = require('minio');
var request = require('request');
let config = require('./config');

var minioClient = new Minio.Client({
    endPoint: config.minio.host,
    port: config.minio.port,
    useSSL: config.minio.useSSL,
    accessKey: config.minio.accessKey,
    secretKey: config.minio.secretKey
});

var headers = { 'Content-Type' : 'application/json' };

Array.prototype.last = function(){
    return this[this.length - 1];
};

var auth = function(){
	return new Promise(function(resolve, reject){
		var options = {
			url: config.simva.url + '/users/login',
			body: JSON.stringify({ username: config.simva.user, password: config.simva.password }),
			method: 'POST',
			headers: headers
		};

		request(options, function(error, response, body){
			if(error){
				console.log('Cant login');
				reject(error);
			}else{
				try{
					var b = JSON.parse(response.body);
					headers.Authorization = 'Bearer ' + b.token;
					resolve();
				}catch(e){
					console.log('Cant login.');
					reject(e);
				}
			}
		});
	})
};

var getActivities = function(query){
	return new Promise(function(resolve, reject){
		var options = {
			url: config.simva.url + '/activities',
			method: 'GET',
			qs: { searchString: JSON.stringify(query)},
			headers: headers
		};

		request(options, function(error, response, body){
			if(error){
				console.log('Cant get activities');
				reject(error);
			}else{
				var b = JSON.parse(response.body);
				resolve(b);
			}
		});
	});
};

var listFiles = function(folder){
	return new Promise(function(resolve, reject){
		let objectsStream = minioClient.listObjects(config.minio.bucket, folder, false);

		try{
			let traces = [];
			objectsStream.on('data', function(obj) {
				traces.push(obj.name);
			});
			objectsStream.on('end', function() {
			  resolve(traces);
			})
			objectsStream.on('error', function(e) {
			  reject(e);
			});
		}catch(e){
			console.log('ERROR');
			reject(e);
		}
	});
}

var getTraces = function(activityId){
	return listFiles('/' + config.minio.topics_dir + '/traces/_id=' + activityId + '/');
}

function streamToString (stream) {
	const chunks = []
	return new Promise((resolve, reject) => {
		try{
			stream.on('data', chunk => chunks.push(chunk))
			stream.on('error', reject)
			stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
		}catch(e){
			reject(e);
		}
	})
}

var getFile = async function(file){
	return new Promise(function(resolve, reject){
		minioClient.getObject(config.minio.bucket, file, function(err, dataStream) {
			if (err) {
				reject(err);
			}else{
				streamToString(dataStream)
					.then(function(content){
						resolve(content);
					})
					.catch(function(error){
						reject(error);
					});
			}
		});
	});
}

var setFile = function(file, content){
	return new Promise(function(resolve, reject){
		minioClient.putObject(config.minio.bucket, file, content, function(err, etag) {
			if(err){
				reject(err);
			}else{
				resolve(etag);
			}
		});
	});	
}

let getState = function(){
	return new Promise(function(resolve, reject){
		getFile('/state.json')
			.then(function(file){
				resolve(JSON.parse(file));
			})
			.catch(function(){
				resolve({ states: {} });
			});
	});
}

let setState = function(state){
	return new Promise(function(resolve, reject){
		setFile('state.json', JSON.stringify(state))
			.then(function(file){
				resolve(JSON.parse(file));
			})
			.catch(function(){
				resolve({ states: {} });
			});
	});
}

let processing = false;
var proccessTraces = async function(){
	console.log('############# STARTING TRACE PROCESSING #############');
	processing = true;
	let state = await getState();

	try{
		await auth();	
	}catch(e){
		console.log("Unable to auth");
		processing = false;
		return e;
	}
	
	let activities = await getActivities({ type: ['gameplay', 'miniokafka', 'rageminio'] });

	for (var i = activities.length - 1; i >= 0; i--) {
		console.log('## Processing activity: ' + activities[i]._id);
		let tracesToAdd = {};

		if(!state.states[activities[i]._id]){
			state.states[activities[i]._id] = {};
		}

		for (var j = activities[i].owners.length - 1; j >= 0; j--) {
			if(!state.states[activities[i]._id][activities[i].owners[j]]){
				state.states[activities[i]._id][activities[i].owners[j]] = null;
			}

			tracesToAdd[activities[i].owners[j]] = '';
		}

		// The list of trace files is retrieved from the server.

		let traces = [];
		try{
			traces = await getTraces(activities[i]._id);
		}catch(e){
			console.log(e);
		}

		// The list of pending traces to add to each owner is generated

		let processedTraces = 0;
		for (var t = 0; t < traces.length && processedTraces < config.batchSize; t++) {
			let addTraceTo = [];

			for (var j = activities[i].owners.length - 1; j >= 0; j--) {
				if(!state.states[activities[i]._id][activities[i].owners[j]]
					|| traces[t] > state.states[activities[i]._id][activities[i].owners[j]]){
					addTraceTo.push(activities[i].owners[j]);
				}
			}

			if(addTraceTo.length > 0){
				processedTraces++;
				try{
					let content = (await getFile(traces[t])).replace('\n','');
					for (var j = 0; j < addTraceTo.length; j++) {
						tracesToAdd[addTraceTo[j]] += (content + ',');
						state.states[activities[i]._id][addTraceTo[j]] = traces[t];
					}
				}catch(e){
					console.log("Error processing traces");
					processing = false;
					return e;
				}
			}
		}

		for (let username of Object.keys(tracesToAdd)) {
			if(tracesToAdd[username] === ''){
				continue;
			}

			/*let n = 0;

			try{
				let files = await listFiles('/' + config.minio.users_dir + '/' + username + '/' + activities[i]._id + '/');
				n = parseInt(files[files.length-1].split('/').last().split('.')[0]) + 1;
			}catch(e){
				console.log('Unable to found last traces file');
			}

			let traces = '[' + tracesToAdd[username].slice(0, -1) + ']';*/

			let traces = [];
			let tracesfile = config.minio.users_dir + '/' + username + '/' + activities[i]._id + '/' + config.minio.traces_file;
			try{
				let rawtraces = await getFile(tracesfile, traces);
				traces = rawtraces.slice(0, -1) + ',' + tracesToAdd[username].slice(0, -1) + ']';
			}catch(e){
				traces = '[' + tracesToAdd[username].slice(0, -1) + ']';
			}

			try{
				await setFile(tracesfile, traces);
			}catch(e){
				console.log(traces);
				console.log("Error saving traces file: " + tracesfile);
				console.log(e);
				processing = false;
				return e;
			}
			await setState(state);
		}
	}

	console.log(state);

	await setState(state);
	processing = false;
	console.log('#####################################################');
}

console.log('#### CURRENT CONFIG ####')
console.log(JSON.stringify(config, null, 2));

setInterval(function(){
	if(!processing){
		try{
			proccessTraces();
		}catch(e){
			processing = false;
			console.log('####### ERROR WHILE PROCESSING THE TRACES. !!!!!!');
		}
	}else{
		console.log('Still processing');
	}
	
}, config.refreshInterval);