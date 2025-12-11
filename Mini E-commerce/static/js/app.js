function toggleCategory(el) {
    let productsDiv = el.nextElementSibling;
    if (productsDiv.style.display === "none") {
        productsDiv.style.display = "flex";
    } else {
        productsDiv.style.display = "none";
    }
}
