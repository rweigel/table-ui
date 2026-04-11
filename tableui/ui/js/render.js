const renderFunctions = {}

renderFunctions._constrainedSearchLinks = function (columnName, columnString) {
  const links = []
  links.push(renderFunctions._searchLink(columnName, columnString, '='))
  links.push('&hairsp;' + renderFunctions._searchLink(columnName, columnString, '>'))
  links.push('&hairsp;' + renderFunctions._searchLink(columnName, columnString, '≥'))
  links.push('&hairsp;' + renderFunctions._searchLink(columnName, columnString, '<'))
  links.push('&hairsp;' + renderFunctions._searchLink(columnName, columnString, '≤'))
  return links
}

renderFunctions._searchLink = function (columnName, columnString, constraint) {
  const constraintIcon = constraint || '🔍'
  if (!constraint || constraint === '=') {
    constraint = ''
  }
  let attrs = 'title="Search column for this exact value"'
  if (constraint !== '') {
    attrs = `title="Search columns for datetimes ${constraint} ${columnString}"`
  }
  attrs += ' style="text-decoration:none;"'
  attrs += ` onclick="triggerSearch('${columnName}', '${constraint}${columnString}')"`
  const url = `#${columnName}=${constraint}${columnString}`
  return `<a href="${url}" ${attrs}>${constraintIcon}</a>`
}

renderFunctions._combineLinks = function (columnString, links, split, wrapperClass) {
  if (columnString === '') {
    return ''
  }
  if (!links) {
    return columnString
  }
  if (typeof links === 'string') {
    links = [links]
  }
  if (!split) {
    split = ''
  }
  if (!wrapperClass) {
    wrapperClass = ''
  }
  return `${columnString}${split}<span class="${wrapperClass}"><nobr>${links.join('')}</nobr></span>`
}

renderFunctions.constrainedSearch = function (columnName, config) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    wrapperClass = 'timeSearchConstraints'
    split = '<br>'
    let links = renderFunctions._constrainedSearchLinks(columnName, columnString)
    return renderFunctions._combineLinks(columnString, links, split, wrapperClass)
  }
}

renderFunctions.ellipsis = function (columnName, config, n) {
  // https://github.com/DataTables/Plugins/blob/master/dataRender/ellipsis.js
  // Previous version used only this line
  //  return window.DataTable.render.ellipsis(n || 30)
  // This version allows click to expand text.
  let esc = function (t) {
      return ('' + t)
          .replace(/&/g, '&amp;')
          .replace(/</g, '&lt;')
          .replace(/>/g, '&gt;')
          .replace(/"/g, '&quot;');
  };

  return (columnString, type, row, meta) => {
    if (type !== 'display' || typeof columnString !== 'string') {
      return columnString
    }
    n = n || 30
    if (n > columnString.length) {
      return columnString
    }
    shortened = columnString.substring(0, n)
    const ellipsisClick = '<span class="ellipsis-click">&#8230;</span>'
    const ellipsisFull = `<span style="display:none;" class="ellipsis-full">${columnString}</span>`
    shortened += ellipsisClick
    return `<span class="ellipsis" title="${columnString}">${shortened}</span>${ellipsisFull}`
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

renderFunctions.splitArray = function (columnName, config, symbol) {
  return (columnString, type, row, meta) => {
    if (type !== 'display') {
      return columnString
    }
    if (typeof columnString === 'string') {
      try {
        const parsed = JSON.parse(columnString.replace(/'/g, '"'))
        if (Array.isArray(parsed)) {
          columnString = JSON.stringify(parsed, null, 2).replace(/\n/g, '<br>')
          columnString = `<pre style="text-align: left;">${columnString}</pre>`
        }
        return columnString
      } catch (e) {
        // not JSON — leave original string
      }
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

renderFunctions._trimURL = function (url, trim) {
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
    return renderFunctions._trimURL(columnString, trim)
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
      options.modify.remove = options.modify.remove || null
      options.modify.replace = options.modify.replace || ''
      //console.log('renderLink:', options)
      if (options.modify.remove) {
        url = url.replace(options.modify.remove, options.modify.replace)
        if (typeof remove === 'string') {
          //if (remove.startsWith('^')) {
          //  remove = new RegExp(remove)
          //}
        }
      }
    }
    let urlTrimmed = url
    if (options.trim) {
      urlTrimmed = renderFunctions._trimURL(url, options.trim)
    }
    const attrs = `href="${url}" title="${url}"`
    columnString = `<a ${attrs} target="_blank">${urlTrimmed}</a>`
    return columnString
  }
}
