import wretch from "wretch";
import QueryStringAddon from "wretch/addons/queryString";

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

const DEFAULT_HEADERS = {
	'Content-Type': 'application/json',
	'Accept': 'application/json'
};


export class SimvaClient {
    /**
     * @param {SimvaOpts} opts
     */
    constructor(opts) {
		this.#opts = opts;
        this.#endpoint = `${opts.protocol}://${opts.host}${opts.port !== undefined ? `:${opts.port}` : ''}`;
		this.#api = wretch(this.#endpoint).errorType("json").resolve(r => r.json());
    }

    /** @type {SimvaOpts} */
    #opts;

    /** @type {string} */
    #endpoint;

	#api;

	/** @type {string} */
	#bearer;


	async #auth(){
		if (this.#bearer !== undefined) return;
		const payload = { username: this.#opts.username, password: this.#opts.password };
		
		const result = /** @type {{token:string}}*/(await this.#api.headers(DEFAULT_HEADERS).url('/users/login').post(payload));
		this.#bearer = result.token;
	};

	/**
	 * 
	 * @param {object} query 
	 * @returns {Promise<Activity[]>}
	 */
	async getActivities(query){
		await this.#auth();
		const activitiesAPI = this.#api.headers(DEFAULT_HEADERS).auth(`Bearer ${this.#bearer}`).addon(QueryStringAddon).url('/activities');
		const searchParam = JSON.stringify(query);
		const activities = /** @type {unknown[]} */ (await activitiesAPI.query({searchString: searchParam}).get());
		for(const activity of activities){
			assertActivity(activity);
		}
		return activities;
	};
}
