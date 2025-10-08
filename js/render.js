function renderColumn (columnName, tableConfig) {
  if (columnName === 'datasetID') {
    _renderColumn.columnName = columnName
    _renderColumn.tableConfig = tableConfig

    const tableName = tableConfig.tableUI.tableMetadata.name
    if (!tableName) {
      return
    }
    if (tableName.startsWith('cdaweb') || tableName.startsWith('spase')) {
      return _renderColumn
    }
  }

  function _renderColumn (columnString, type, row, meta) {
    if (type === 'display') {
      //console.log(columnString, type, row, meta)
      console.log(_renderColumn.tableConfig)
      const columnNames = _renderColumn.tableConfig.columns.map(c => c.name)

      // TODO: Not all have "_v01".
      const base = 'https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/'
      const fnameCDF = base + '0MASTERS/' + columnString.toLowerCase() + '_00000000_v01.cdf'
      const fnameJSON = base + '0JSONS/' + columnString.toLowerCase() + '_00000000_v01.json'
      const fnameSKT = base + '0SKELTABLES/' + columnString.toLowerCase() + '_00000000_v01.skt'
      const fnameHAPI1 = `https://cdaweb.gsfc.nasa.gov/hapi/info?id=${columnString}`
      const fnameHAPI2 = `https://cottagesystems.com/server/cdaweb/hapi/info?id=${columnString}`

      columnString = `${columnString}`
      columnString += '<span style="font-size:0.75em">'
      columnString += ` <a href="${fnameCDF}"   title="Master CDF" target="_blank">M</a>`
      columnString += ` <a href="${fnameJSON}"  title="Master JSON" target="_blank">J</a>`
      columnString += ` <a href="${fnameSKT}"   title="Master Skeleton Table" target="_blank">SK</a>`
      columnString += ` <a href="${fnameHAPI1}" title="HAPI Info" target="_blank">H<sub>1</sub></a>`
      columnString += ` <a href="${fnameHAPI2}" title="HAPI Info Dev Server" target="_blank">H<sub>2</sub></a>`

      const tableName = _renderColumn.tableConfig.tableUI.tableMetadata.name
      if (tableName === 'spase.dataset') {
        console.log(row[1].replace('spase://', 'https://spase-group.org/'))
        const fnameSPASE = row[1].replace('spase://', 'https://spase-group.org/') + '.json'
        columnString += ` <a href="${fnameSPASE}" title="SPASE" target="_blank">SP</a>`
      }

      if (tableName === 'cdaweb.dataset') {
        const index = columnNames.indexOf('spase_DatasetResourceID')
        const fnameSPASE = row[index].replace('spase://', 'https://hpde.io/') + '.json'
        columnString += `&nbsp;<a href="${fnameSPASE}" title="SPASE">SP</a>`
      }

      columnString += '</span>'

      return columnString
    }
    return columnString
  }
}

function renderTableMetadata (config) {
  console.log('renderTableMetadata(): config:')

  if (!config.tableUI.tableMetadata) return ''

  let description = config.tableUI.tableMetadata.description || ''
  if (description) {
    description = `<p>${description}<p>`
  }

  let name = config.tableUI.tableMetadata.name || ''
  if (name) {
    name = `<code>${name}</code>`
  }

  let creationDate = config.tableUI.tableMetadata.creationDate || ''
  if (creationDate) {
    creationDate = ` | Created ${creationDate}.`
  }

  const base = window.location.origin + window.location.pathname.replace(/\/$/, '')
  let txt = ` Table ${name}: <a href="${base}/config" title="${base}/config" target="_blank">config</a>`
  let href = ''
  if (config.tableUI.sqldb) {
    href = `${base}/sqldb`
  }
  if (config.tableUI.jsondb) {
    href = `${base}/jsondb`
  }
  txt += ` | <a href="${href}" title="${href}" target="_blank">data</a>`
  txt += creationDate
  txt += description
  return txt
}
