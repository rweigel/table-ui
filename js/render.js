const renderFunctions = {}

renderFunctions.ellipsis = function (columnName, config, n) {
  return window.DataTable.render.ellipsis(n || 30)
}
renderFunctions.renderLink = function (columnName, config, remove) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    const url = columnString.replace(remove, '')
    columnString = `<a href="${columnString}" title="${columnString}" target="_blank">${url}</a>`
    return columnString
  }
}
renderFunctions.renderURI = function (columnName, config, prefix, prefixReplace, trim) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    if (columnString.startsWith(prefix)) {
      const url = columnString.replace(prefix, prefixReplace)
      let shortURL = url.replace(prefixReplace, '')
      if (trim) {
        shortURL = columnString.split('/')
        shortURL = shortURL[shortURL.length - 1]
      }
      columnString = `<a href="${url}" title="${url}" target="_blank">${shortURL}</a>`
    }
    return columnString
  }
}

renderFunctions.trimURL = function (columnName, config) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    const urlSplit = columnString.split('/')
    if (urlSplit[urlSplit.length - 1] !== '') {
      const attrs = `href="${columnString}" title="${columnString}"`
      let urlShort = urlSplit[urlSplit.length - 1]
      if (urlShort.startsWith('?')) {
        urlShort = '…/' + urlShort
      }
      urlShort = `<a ${attrs} target="_blank">${urlShort}</a>`
      return urlShort
    }
    return columnString
  }
}

renderFunctions.underline = function (columnName, config) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    if (columnString) {
      let style = 'text-decoration-line: underline;'
      style += 'text-decoration-style: wavy;'
      return `<div style="${style}">${columnString}</div>`
    }
    return columnString
  }
}

renderFunctions.annotate = function (columnName, config, symbol) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    if (columnString === 'a02') {
      return `${symbol}${columnString}`
    }
    return columnString
  }
}

renderFunctions.bold = function (columnName, config) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    if (!columnString) {
      return columnString
    }
    return `<span style="font-weight:bold">${columnString}</span>`
  }
}
