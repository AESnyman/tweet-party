

mapping = {
          "properties": {
            "created_at": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "date": {
              "type": "date",
              "fields": {
                "keyword": {
                  "type": "keyword"
                }
              }
            },
            "sentiment_tb": {
              "type": "long"
            },
            "sentiment_tbnb": {
              "type": "long"
            },
            "entities": {
              "properties": {
                "hashtags": {
                  "properties": {
                    "indices": {
                      "type": "long"
                    },
                    "text": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    }
                  }
                },
                "media": {
                  "properties": {
                    "display_url": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "expanded_url": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "id": {
                      "type": "long"
                    },
                    "id_str": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "indices": {
                      "type": "long"
                    },
                    "media_url": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "media_url_https": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "sizes": {
                      "properties": {
                        "large": {
                          "properties": {
                            "h": {
                              "type": "long"
                            },
                            "resize": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "w": {
                              "type": "long"
                            }
                          }
                        },
                        "medium": {
                          "properties": {
                            "h": {
                              "type": "long"
                            },
                            "resize": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "w": {
                              "type": "long"
                            }
                          }
                        },
                        "small": {
                          "properties": {
                            "h": {
                              "type": "long"
                            },
                            "resize": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "w": {
                              "type": "long"
                            }
                          }
                        },
                        "thumb": {
                          "properties": {
                            "h": {
                              "type": "long"
                            },
                            "resize": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "w": {
                              "type": "long"
                            }
                          }
                        }
                      }
                    },
                    "type": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "url": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    }
                  }
                },
                "urls": {
                  "properties": {
                    "display_url": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "expanded_url": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "indices": {
                      "type": "long"
                    },
                    "url": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    }
                  }
                },
                "user_mentions": {
                  "properties": {
                    "id": {
                      "type": "long"
                    },
                    "id_str": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "indices": {
                      "type": "long"
                    },
                    "name": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "screen_name": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    }
                  }
                }
              }
            },
            "extended_entities": {
              "properties": {
                "media": {
                  "properties": {
                    "display_url": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "expanded_url": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "id": {
                      "type": "long"
                    },
                    "id_str": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "indices": {
                      "type": "long"
                    },
                    "media_url": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "media_url_https": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "sizes": {
                      "properties": {
                        "large": {
                          "properties": {
                            "h": {
                              "type": "long"
                            },
                            "resize": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "w": {
                              "type": "long"
                            }
                          }
                        },
                        "medium": {
                          "properties": {
                            "h": {
                              "type": "long"
                            },
                            "resize": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "w": {
                              "type": "long"
                            }
                          }
                        },
                        "small": {
                          "properties": {
                            "h": {
                              "type": "long"
                            },
                            "resize": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "w": {
                              "type": "long"
                            }
                          }
                        },
                        "thumb": {
                          "properties": {
                            "h": {
                              "type": "long"
                            },
                            "resize": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "w": {
                              "type": "long"
                            }
                          }
                        }
                      }
                    },
                    "type": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    },
                    "url": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    }
                  }
                }
              }
            },
            "favorite_count": {
              "type": "long"
            },
            "favorited": {
              "type": "boolean"
            },
            "id": {
              "type": "long"
            },
            "id_str": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "state": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "state_name": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "in_reply_to_screen_name": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "in_reply_to_status_id": {
              "type": "long"
            },
            "in_reply_to_status_id_str": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "in_reply_to_user_id": {
              "type": "long"
            },
            "in_reply_to_user_id_str": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "is_quote_status": {
              "type": "boolean"
            },
            "lang": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "metadata": {
              "properties": {
                "iso_language_code": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "result_type": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "place": {
              "properties": {
                "attributes": {
                  "type": "object"
                },
                "bounding_box": {
                  "properties": {
                    "coordinates": {
                      "type": "float"
                    },
                    "type": {
                      "type": "text",
                      "fields": {
                        "keyword": {
                          "type": "keyword",
                          "ignore_above": 256
                        }
                      }
                    }
                  }
                },
                "country": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "country_code": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "full_name": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "id": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "name": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "place_type": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "url": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "possibly_sensitive": {
              "type": "boolean"
            },
            "retweet_count": {
              "type": "long"
            },
            "retweeted": {
              "type": "boolean"
            },
            "source": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "text": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "truncated": {
              "type": "boolean"
            },
            "user": {
              "properties": {
                "contributors_enabled": {
                  "type": "boolean"
                },
                "created_at": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "default_profile": {
                  "type": "boolean"
                },
                "default_profile_image": {
                  "type": "boolean"
                },
                "description": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "entities": {
                  "properties": {
                    "description": {
                      "properties": {
                        "urls": {
                          "properties": {
                            "display_url": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "expanded_url": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "indices": {
                              "type": "long"
                            },
                            "url": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            }
                          }
                        }
                      }
                    },
                    "url": {
                      "properties": {
                        "urls": {
                          "properties": {
                            "display_url": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "expanded_url": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            },
                            "indices": {
                              "type": "long"
                            },
                            "url": {
                              "type": "text",
                              "fields": {
                                "keyword": {
                                  "type": "keyword",
                                  "ignore_above": 256
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                },
                "favourites_count": {
                  "type": "long"
                },
                "follow_request_sent": {
                  "type": "boolean"
                },
                "followers_count": {
                  "type": "long"
                },
                "following": {
                  "type": "boolean"
                },
                "friends_count": {
                  "type": "long"
                },
                "geo_enabled": {
                  "type": "boolean"
                },
                "has_extended_profile": {
                  "type": "boolean"
                },
                "id": {
                  "type": "long"
                },
                "id_str": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "is_translation_enabled": {
                  "type": "boolean"
                },
                "is_translator": {
                  "type": "boolean"
                },
                "lang": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "listed_count": {
                  "type": "long"
                },
                "location": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "name": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "notifications": {
                  "type": "boolean"
                },
                "profile_background_color": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "profile_background_image_url": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "profile_background_image_url_https": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "profile_background_tile": {
                  "type": "boolean"
                },
                "profile_banner_url": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "profile_image_url": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "profile_image_url_https": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "profile_link_color": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "profile_sidebar_border_color": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "profile_sidebar_fill_color": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "profile_text_color": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "profile_use_background_image": {
                  "type": "boolean"
                },
                "protected": {
                  "type": "boolean"
                },
                "screen_name": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "statuses_count": {
                  "type": "long"
                },
                "translator_type": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "url": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "verified": {
                  "type": "boolean"
                }
              }
            }
          }
        }
