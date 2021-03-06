let config = {}

config.batchSize = process.env.BATCH_SIZE || 100
config.refreshInterval = process.env.REFRESH_INTERVAL || 5000

config.minio = {}
config.minio.host = process.env.MINIO_HOST || 'minio.simva.e-ucm.es'
config.minio.useSSL = process.env.MINIO_SSL || true
config.minio.port = process.env.MINIO_PORT || (config.minio.useSSL ? 443 : 80)
config.minio.accessKey = process.env.MINIO_ACCESS_KEY || 'root'
config.minio.secretKey = process.env.MINIO_SECRET_KEY || 'password'
config.minio.bucket = process.env.MINIO_BUCKET || 'datalake'

config.simva = {}
config.simva.host = process.env.SIMVA_HOST || 'api.simva.e-ucm.es'
config.simva.protocol = process.env.SIMVA_PROTOCOL || 'https'
config.simva.port = process.env.SIMVA_PORT || '443'
config.simva.url = config.simva.protocol + '://' + config.simva.host + ':' + config.simva.port;

module.exports = config;