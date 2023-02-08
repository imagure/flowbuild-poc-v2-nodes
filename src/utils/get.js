const getPropByPath = (object, path) => {
    const _path = Array.isArray(path)
        ? path
        : path.split('.');
    if (object && _path.length) return getPropByPath(object[_path.shift()], _path);
    return object;
}

module.exports = {
    get: getPropByPath
}