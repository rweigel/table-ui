function render (tableName, columnName) {
  if (columnName === 'datasetID') {
    if (tableName.startsWith('cdaweb')) {
      return cdaweb
    }
    if (tableName.startsWith('spase')) {
      return cdaweb
    }
  }

  function cdaweb (data, type, full, meta) {
    if (type === 'display') {
      // TODO: Not all have "_v01".
      const base = 'https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/'
      const fnameCDF = base + '0MASTERS/' + data.toLowerCase() + '_00000000_v01.cdf'
      const fnameJSON = base + '0JSONS/ ' + data.toLowerCase() + '_00000000_v01.json'
      const fnameSKT = base + '0SKELTABLES/ ' + data.toLowerCase() + '_00000000_v01.skt'
      data = `${data}`
      data += `&nbsp;<a href="${fnameCDF}" title="Master CDF">M</a>`
      data += `&nbsp;<a href="${fnameJSON}" title="JSON">J</a>`
      data += `&nbsp;<a href="${fnameSKT}" title="Skeleton Table">S</a>`
      return data
    }
    return data
  }
}
