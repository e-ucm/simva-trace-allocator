import wretch from "wretch";

/**
 * @typedef SimvaOpts
 * @property {string} host SimVA API host
 * @property {string} protocol SimVA API protocol
 * @property {number} [port] SimVA API port
 * @property {string} username OAuth2 resource owner username (the `garbagecollector` user)
 * @property {string} password OAuth2 resource owner password
 * @property {string} ssoHost Keycloak host
 * @property {string} ssoProtocol Keycloak protocol
 * @property {number} [ssoPort] Keycloak port
 * @property {string} ssoRealm Keycloak realm
 * @property {string} clientId Keycloak client id of this service
 * @property {string} clientSecret Keycloak client secret of this service
 */

/**
 * @typedef Activity
 * @property {string} _id
 * @property {number} activity_id
 * @property {number} session_id
 * @property {number} simlet_id
 * @property {string} activity_name
 * @property {string} activity_type
 */

/**
 * @typedef TokenResponse
 * @property {string} access_token
 * @property {string} [refresh_token]
 * @property {number} expires_in
 */

/**
 * Validates an activity returned by the API and normalizes its id.
 * The API exposes the id as `activity_id`, while the rest of the allocator
 * refers to activities by `_id`, so both names are kept in sync here.
 *
 * @param {unknown} maybeActivity
 * @param {boolean} [checkTypes=false]
 * @returns {asserts maybeActivity is Activity}
 */
function assertActivity(maybeActivity, checkTypes = false) {
	if (typeof maybeActivity !== 'object' || maybeActivity === null) throw new Error('Not an Activity');
	if ('activity_id' in maybeActivity && maybeActivity.activity_id !== undefined) {
		maybeActivity._id = String(maybeActivity.activity_id);
	}
	if (! ('_id' in maybeActivity)) throw new Error('Not an Activity');
}

const APPLICATION_JSON_MIME_TYPE = 'application/json';
const FORM_URL_ENCODED_MIME_TYPE = 'application/x-www-form-urlencoded';

/**
 * Access tokens are renewed this many milliseconds before they actually expire,
 * so a token is never used in a request that could expire while it is in flight.
 */
const TOKEN_EXPIRY_SKEW_MS = 60 * 1000;

/**
 * The Keycloak stack generates the client id/secret of this service and the
 * password of the garbage collector user, so both have to be exported from
 * docker-stacks/config/keycloak/simva-env.sh before starting the service.
 */
const GENERATED_CREDENTIALS_HINT = 'The Keycloak stack generates these credentials into docker-stacks/config/keycloak/simva-env.sh, source it before starting the service';

/**
 * Returns the OAuth2 error reported by the token endpoint, if any.
 *
 * @param {any} err - The error thrown by the token endpoint
 * @returns {string | undefined}
 */
function oauthError(err) {
	return typeof err?.json?.error === 'string' ? err.json.error : undefined;
}

/**
 * Explains the most common token endpoint failures, which are almost always a
 * deployment issue (credentials that were never exported) rather than a bug.
 *
 * @param {any} err - The error thrown by the token endpoint
 * @param {SimvaOpts} opts - Options in use, to name the rejected credentials
 * @returns {any} The error to throw
 */
function explainTokenError(err, opts) {
	const error = oauthError(err);
	if (error === 'invalid_client') {
		return new Error(`Keycloak rejected the client credentials of this service (client_id '${opts.clientId}'): check SIMVA_CLIENT_ID / SIMVA_CLIENT_SECRET. ${GENERATED_CREDENTIALS_HINT}.`, { cause: err });
	}
	if (error === 'invalid_grant') {
		return new Error(`Keycloak rejected the resource owner credentials (user '${opts.username}'): check SIMVA_USER / SIMVA_PASSWORD. ${GENERATED_CREDENTIALS_HINT}.`, { cause: err });
	}
	return err;
}

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
		const ssoEndpoint = `${opts.ssoProtocol}://${opts.ssoHost}${opts.ssoPort !== undefined ? `:${opts.ssoPort}` : ''}`;
		this.#tokenUrl = `${ssoEndpoint}/realms/${opts.ssoRealm}/protocol/openid-connect/token`;
		this.#tokenApi = wretch(this.#tokenUrl)
			.content(FORM_URL_ENCODED_MIME_TYPE)
			.accept(APPLICATION_JSON_MIME_TYPE);
		this.#accessToken = undefined;
		this.#refreshToken = undefined;
		this.#accessTokenExpiresAt = 0;
    }

    /** @type {SimvaOpts} */
    #opts;

    /** @type {string} */
    #endpoint;

	#api;

	/** @type {string} */
	#tokenUrl;

	#tokenApi;

	/** @type {string|undefined} */
	#accessToken;

	/** @type {string|undefined} */
	#refreshToken;

	/** @type {number} */
	#accessTokenExpiresAt;

	/**
	 * Requests an access token using the OAuth2 resource owner password
	 * credentials grant, so the allocator authenticates directly against
	 * Keycloak instead of relying on a SimVA login endpoint.
	 *
	 * @returns {Promise<string>} The authorization header value
	 */
	async #auth(){
		const form = new URLSearchParams({
			grant_type: 'password',
			client_id: this.#opts.clientId,
			client_secret: this.#opts.clientSecret,
			username: this.#opts.username,
			password: this.#opts.password,
			scope: 'openid'
		});
		/** @type {TokenResponse} */
		let result;
		try {
			result = await this.#tokenApi.post(form.toString()).json();
		} catch (err) {
			throw explainTokenError(err, this.#opts);
		}
		this.#storeToken(result);
		return this.#authorization();
	}

	/**
	 * Renews the access token with the OAuth2 refresh token grant.
	 *
	 * @returns {Promise<string>} The authorization header value
	 */
	async #refresh(){
		if (this.#refreshToken === undefined) {
			return await this.#auth();
		}
		try {
			const form = new URLSearchParams({
				grant_type: 'refresh_token',
				client_id: this.#opts.clientId,
				client_secret: this.#opts.clientSecret,
				refresh_token: this.#refreshToken
			});
			/** @type {TokenResponse} */
			const result = await this.#tokenApi.post(form.toString()).json();
			// Keycloak only rotates the refresh token when it is about to expire,
			// so the previous one is kept when the response omits it
			if (result.refresh_token !== undefined) {
				this.#refreshToken = result.refresh_token;
			}
			this.#storeToken(result);
		} catch (e) {
			// Rejected credentials are not a problem of the refresh token, no retry helps
			if (oauthError(e) !== undefined) {
				throw explainTokenError(e, this.#opts);
			}
			// A rejected refresh token cannot be recovered from, ask for a new one
			this.#accessToken = undefined;
			this.#refreshToken = undefined;
			this.#accessTokenExpiresAt = 0;
			return await this.#auth();
		}
		return this.#authorization();
	}

	/**
	 * Stores an access token and the moment it stops being usable.
	 *
	 * @param {TokenResponse} result
	 * @returns {void}
	 */
	#storeToken(result){
		if (typeof result.access_token !== 'string') {
			throw new Error('Keycloak did not return an access token');
		}
		this.#accessToken = result.access_token;
		this.#accessTokenExpiresAt = Date.now() + (result.expires_in * 1000) - TOKEN_EXPIRY_SKEW_MS;
	}

	/** @returns {string} */
	#authorization(){
		return `Bearer ${this.#accessToken}`;
	}

	/**
	 * Returns a usable authorization header, renewing the access token first
	 * when the current one is missing or about to expire.
	 *
	 * @returns {Promise<string>}
	 */
	async #authorizationHeader(){
		if (this.#accessToken === undefined) {
			return await this.#auth();
		}
		if (Date.now() >= this.#accessTokenExpiresAt) {
			return await this.#refresh();
		}
		return this.#authorization();
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

		const token = await this.#authorizationHeader();
		const activitiesAPI = this.#api.url(`/activities?${queryParams.toString()}`)
		.auth(token);
		/** @type {Activity[]} */
		const activities = await activitiesAPI.get()
		.unauthorized(async (error, req) => {
			// The token was rejected before its expected expiry, renew it and replay
			const renewedToken = await this.#refresh();
			return req.auth(renewedToken).get().unauthorized((nestedError) => {
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
