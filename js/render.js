const renderFunctions = {}

renderFunctions.ellipsis = function (columnName, config, n) {
  return window.DataTable.render.ellipsis(n || 30)
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

function trimURL (url, trim) {
  const attrs = `href="${url}" title="${url}"`
  let urlTrimmed = url
  if (trim !== undefined) {
    urlTrimmed = url.replace(trim, '')
  } else {
    const urlSplit = url.split('/')
    if (urlSplit[urlSplit.length - 1] !== '') {
      urlTrimmed = urlSplit[urlSplit.length - 1]
      if (urlTrimmed.startsWith('?')) {
        urlTrimmed = '…/' + urlTrimmed
      }
    }
  }
  return `<a ${attrs} target="_blank">${urlTrimmed}</a>`
}

renderFunctions.trimURL = function (columnName, config, trim) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    return trimURL(columnString, trim)
  }
}

renderFunctions.renderLink = function (columnName, config, options) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    options = options || {}
    let url = columnString
    if (options.modify) {
      options.remove = options.remove || null
      options.replace = options.replace || ''
      console.log('renderLink:', options)
      if (options.remove) {
        url = columnString.replace(options.remove, options.replace)
        if (typeof remove === 'string') {
          //if (remove.startsWith('^')) {
          //  remove = new RegExp(remove)
          //}
        }
      }
    }
    let urlTrimmed = url
    if (options.trim) {
      urlTrimmed = trimURL(url, options.trim)
    }
    const attrs = `href="${url}" title="${url}"`
    columnString = `<a ${attrs} target="_blank">${urlTrimmed}</a>`
    return columnString
  }
}
