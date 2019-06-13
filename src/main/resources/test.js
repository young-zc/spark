let version1 = 'v1.2.97a';
let version2 = 'v1.10.1b';
let versionArr1 = version1.replace(/[a-zA-Z]/g, (match,i)=> '.' + match.charCodeAt()).split(/[^\d]/);
let versionArr2 = version2.replace(/[a-zA-Z]/g, (match,i)=> '.' + match.charCodeAt()).split(/[^\d]/);

// 保证两个数据长度一样，面向 `v1.2` 和 `v1.2.3` 这样的情况
if(versionArr1.length > versionArr2.length) {
    versionArr2.splice(versionArr2.length, 0, ...Array(versionArr1.length-versionArr2.length).fill(0))
} else {
    versionArr1.splice(versionArr1.length, 0, ...Array(versionArr2.length-versionArr1.length).fill(0))
}

// 按节比较
let result = 'version1 equels version2.'
for(let i=0 ; i < versionArr1.length; i ++) {
    if (+versionArr1[i] > +versionArr2[i]) {
        result = 'version1 is bigger.';
        break;
    } else if (+versionArr1[i] < +versionArr2[i]){
        result = 'version2 is bigger.';
        break;
    }
}
console.log(result);
