package org.yamcs.yarch;

public class ArrayDataType extends DataType {

    private final DataType elementType;

    protected ArrayDataType(DataType elementType) {
        super(_type.ARRAY, ARRAY_ID);
        this.elementType = elementType;
    }

    public DataType getElementType() {
        return elementType;
    }

    @Override
    public int hashCode() {
        return 31 + elementType.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ArrayDataType other = (ArrayDataType) obj;
        if (elementType == null) {
            if (other.elementType != null)
                return false;
        } else if (!elementType.equals(other.elementType))
            return false;
        return true;
    }

}
