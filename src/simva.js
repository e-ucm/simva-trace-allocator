import wretch from "wretch";

/**
 * @typedef SimvaOpts
 * @property {string} host
 * @property {string} protocol
 * @property {number} [port]
 * @property {string} username
 * @property {string} password
 */

/**
 * @typedef Activity
 * @property {string} _id
 * @property {string[]} owners
 */

/**
 * @param {unknown} maybeActivity
 * @param {boolean} [checkTypes=false]
 * @returns {asserts maybeActivity is Activity}
 */
function assertActivity(maybeActivity, checkTypes = false) {
	if (typeof maybeActivity !== 'object') throw new Error('Not an Activity');
	if (! ('_id' in maybeActivity)) throw new Error('Not an Activity');
	if (! ('owners' in maybeActivity) || ! Array.isArray(maybeActivity.owners)) throw new Error('Not an Activity');
}

const APPLICATION_JSON_MIME_TYPE = 'application/json';

export class SimvaClient {
    /**
     * @param {SimvaOpts} opts
     */
    constructor(opts) {
		this.#opts = opts;
        this.#endpoint = `${opts.protocol}://${opts.host}${opts.port !== undefined ? `:${opts.port}` : ''}`;
		this.#api = wretch(this.#endpoint)
			.content(APPLICATION_JSON_MIME_TYPE)
			.accept(APPLICATION_JSON_MIME_TYPE);
		this.#bearer = '';
    }

    /** @type {SimvaOpts} */
    #opts;

    /** @type {string} */
    #endpoint;

	#api;

	/** @type {string} */
	#bearer;

	async #auth(){
		const payload = { username: this.#opts.username, password: this.#opts.password };		
		/** @type {{token:string}}*/
		const result = await this.#api.url('/users/login').post(payload).json();
		this.#bearer = `Bearer ${result.token}`;
		return this.#bearer;
	}

	/**
	 * 
	 * @param {object} query 
	 * @returns {Promise<Activity[]>}
	 */
	async getActivities(query){
		const searchParam = JSON.stringify(query);;
		const queryParams = new URLSearchParams();
		queryParams.append('searchString', searchParam);

		const activitiesAPI = this.#api.url(`/activities?${queryParams.toString()}`)
		.auth(this.#bearer);
		/** @type {Activity[]} */
		const activities = await activitiesAPI.get()
		.unauthorized(async (error, req) => {
			// Renew credentials
			const token = await this.#auth();
			// Replay the original request with new credentials
			return req.auth(token).get().unauthorized((nestedError) => {
			  	throw nestedError;
			}).json();
		})
		.json();
		for(const activity of activities){
			assertActivity(activity);
		}
		return activities;
	};
}
