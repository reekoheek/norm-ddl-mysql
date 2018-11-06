const debug = require('debug')('norm-ddl:adapters:mysql');

const TYPES = {
  nstring: 'VARCHAR(255)',
  ninteger: 'INT',
  ndatetime: 'DATETIME',
  nboolean: 'TINYINT',
  nlist: 'TEXT',
};

module.exports = class Sqlite {
  async define (schema, connection) {
    let fieldLines = schema.fields.map(field => {
      let schemaType = field.constructor.name.toLowerCase();
      let dataType = TYPES[schemaType];
      if (!dataType) {
        throw new Error(`Unknown data type mapping from ${schemaType}`);
      }
      let line = `${this.escape(field.name)} ${dataType}`;
      line += ` ${getFilter(field, 'required') ? 'NOT NULL' : 'NULL'}`;
      if (getFilter(field, 'unique')) {
        line += ` UNIQUE`;
      }
      return line;
    });

    fieldLines.unshift(`\`id\` INT PRIMARY KEY AUTO_INCREMENT`);

    let sql = `
CREATE TABLE ${this.escape(schema.name)} (
  ${fieldLines.join(',\n  ')}
)
    `.trim();

    debug(sql);

    let conn = await connection.getConnection();
    await conn.query(sql);
  }

  async undefine (schema, connection) {
    let sql = `DROP TABLE IF EXISTS ${this.escape(schema.name)}`.trim();

    debug(sql);

    let conn = await connection.getConnection();
    await conn.query(sql);
  }

  escape (v) {
    return '`' + v + '`';
  }
};

function getFilter (field, name) {
  return field.rawFilters.find(f => f[0] === name);
}