const avro = require('avsc');

const weatherSchemaAvro = {
  name: 'weatherSchema',
  type: 'record',
  fields: [
    {
      name: 'date',
      type: {
        type: 'long',
        logicalType: 'timestamp-millis'
      }
    },
    {
      name: 'degree',
      type: 'double'
    }
  ]
};

const weather = avro.parse(weatherSchemaAvro);

module.exports = weather;