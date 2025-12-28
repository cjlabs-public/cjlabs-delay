package com.cjlabs.api.business.enums;

import com.cjlabs.domain.enums.IEnumStr;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum HttpMethodEnum implements IEnumStr {

    GET("GET", ""),
    POST("POST", "");

    private final String code;
    private final String msg;


}
